%% =============================================================================
%%  reliable.erl -
%%
%%  Copyright (c) 2020 Christopher Meiklejohn. All rights reserved.
%%  Copyright (c) 2022 Leapsight Technologies Limited. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Reliable is an OTP application that offers a solution to the problem of
%% ensuring a sequence of Riak KV operations are guaranteed to occur, and to
%% occur in order.
%% The problem arises when one wants to write multiple associated objects to
%% Riak KV which does not support multi-key atomicity, including but not
%% exclusively, the update of application-managed secondary indices after a
%% write.
%% @end
%% -----------------------------------------------------------------------------
-module(reliable).
-include_lib("kernel/include/logger.hrl").
-include("reliable.hrl").

-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-define(WORKFLOW_ID, reliable_workflow_id).
-define(WORKFLOW_LEVEL, reliable_workflow_level).
-define(WORKFLOW_GRAPH, reliable_digraph).
-define(WORKFLOW_EVENT_PAYLOAD, reliable_workflow_event_payload).


-type enqueue_opts()    ::  #{
                                work_id => reliable_work:id(),
                                partition_key => binary(),
                                event_payload => any(),
                                subscribe => boolean(),
                                %% riak_pool:opts()
                                deadline => pos_integer(),
                                timeout => pos_integer(),
                                max_retries => non_neg_integer(),
                                retry_backoff_interval_min => non_neg_integer(),
                                retry_backoff_interval_max => non_neg_integer(),
                                retry_backoff_type => jitter | normal
                            }.

-type wf_opts()         ::  #{
                                work_id => reliable_work:id(),
                                partition_key => binary(),
                                event_payload => any(),
                                subscribe => boolean(),
                                on_terminate => fun((Reason :: any()) -> any())
                            }.

-type wf_item()         ::  {
                                Id :: wf_item_id(),
                                {update | delete, wf_action()}
                            }.
-type wf_item_id()      ::  term().

-type wf_action()       ::  reliable_task:new()
                            | fun(() -> reliable_task:new()).
-type wf_fun()          ::  fun(() -> wf_fun_result()).
-type wf_fun_result()   ::  any().
-type wf_result()       ::  #{
                                work_id := reliable_work:id(),
                                work_ref := undefined | reliable_work_ref:t(),
                                result := wf_fun_result(),
                                is_nested := boolean()
                            }.

-export_type([enqueue_opts/0]).
-export_type([wf_item/0]).
-export_type([wf_item_id/0]).
-export_type([wf_opts/0]).
-export_type([wf_action/0]).
-export_type([wf_result/0]).

-export([abort/1]).
-export([add_workflow_items/1]).
-export([add_workflow_precedence/2]).
-export([enqueue/1]).
-export([enqueue/2]).
-export([enqueue/3]).
-export([ensure_in_workflow/0]).
-export([find_workflow_item/1]).
-export([get_workflow_event_payload/0]).
-export([get_workflow_item/1]).
-export([is_in_workflow/0]).
-export([is_nested_workflow/0]).
-export([set_workflow_event_payload/1]).
-export([status/1]).
-export([status/2]).
-export([workflow/1]).
-export([workflow/2]).
-export([workflow_id/0]).
-export([workflow_nesting_level/0]).
-export([yield/1]).
-export([yield/2]).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% > Notice subscriptions are not working at the moment.
%% > See {@link yield/1,2} to track progress.
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Tasks :: [reliable_task:new()]) ->
    {ok, WorkRef :: reliable_work_ref:t()} | {error, term()}.

enqueue(Tasks) ->
    enqueue(Tasks, #{}).


%% -----------------------------------------------------------------------------
%% @doc
%% > Notice subscriptions are not working at the moment
%% > See {@link yield/1,2} to track progress.
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Tasks :: [reliable_task:new()], Opts :: enqueue_opts()) ->
    {ok, WorkRef :: reliable_work_ref:t()} | {error, term()}.

enqueue(Tasks, Opts) ->
    enqueue(Tasks, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% > Notice subscriptions are not working at the moment
%% > See {@link yield/1,2} to track progress.
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    Tasks :: [reliable_task:new()],
    Opts :: enqueue_opts(),
    Timeout :: timeout()) ->
    {ok, WorkRef :: reliable_work_ref:t()} | {error, term()}.

enqueue(Tasks, Opts, Timeout)
when is_list(Tasks) andalso is_map(Opts) andalso ?IS_TIMEOUT(Timeout) ->
    WorkId = get_work_id(Opts),
    PartitionKey = maps:get(partition_key, Opts, undefined),
    EventPayload = maps:get(event_payload, Opts, undefined),

    Work = reliable_work:new(WorkId, Tasks, EventPayload),
    StoreRef = binary_to_atom(reliable_config:partition(PartitionKey), utf8),
    WorkRef = reliable_work:ref(StoreRef, Work),

    Subscribed = maybe_subscribe(WorkRef, Opts),

    try reliable_partition_store:enqueue(StoreRef, Work, Opts, Timeout) of
        {ok, WorkRef} ->
            {ok, WorkRef};
        {error, Reason} ->
            throw(Reason)
    catch
        _:Reason when Subscribed ->
            ok = unsubscribe(WorkRef),
            {error, Reason};
        _:Reason ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the value associated with the key `event_payload' when used as
%% option from a previous {@link enqueue/2}. The calling process is suspended
%% until the work is completed or
%%
%% > The current implementation is not ideal as it recursively reads then status
%% from the database. So do not abuse it.
%% > Also at the moment complete tasks are deleted, so the abscense of a task
%% is considered as either succesful or failed, this will also change as we
%% will be retaining tasks that are discarded or completed.
%% > This will be replaced by a pubsub version soon.
%% @end
%% -----------------------------------------------------------------------------
-spec yield(WorkRef :: reliable_work_ref:t()) ->
    {ok, Payload :: any()} | {error, any()} | timeout.

yield(WorkRef) ->
    yield(WorkRef, 15000).


%% -----------------------------------------------------------------------------
%% @doc Returns the value associated with the key `event_payload' when used as
%% option from a previous {@link enqueue/2} or `timeout' when `Timeout'
%% milliseconds has elapsed.
%%
%% > The current implementation is not ideal as it recursively reads then status
%% from the database. So do not abuse it.
%% > Also at the moment complete tasks are deleted, so the abscense of a task
%% is considered as either succesful or failed, this will also change as we
%% will be retaining tasks that are discarded or completed.
%% > This will be replaced by a pubsub version soon.
%% @end
%% -----------------------------------------------------------------------------
-spec yield(WorkRef :: reliable_work_ref:t(), Timeout :: pos_integer()) ->
    {ok, Payload :: any()} | {error, any()} | timeout.

yield(WorkRef, Timeout) when is_integer(Timeout), Timeout > 0 ->
    yield(WorkRef, Timeout, undefined).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkRef :: reliable_work_ref:t() | binary()) ->
    {in_progress, Status :: reliable_work:status()}
    | {failed, Status :: reliable_work:status()}
    | {error, not_found | badref | any()}.

status(WorkerRef) ->
    reliable_partition_store:status(WorkerRef).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(
    WorkRef :: reliable_work_ref:t() | binary(), Timeout :: timeout()) ->
    {in_progress, Status :: reliable_work:status()}
    | {failed, Status :: reliable_work:status()}
    | {error, not_found | timeout | badref | any()}.

status(WorkerRef, Timeout) ->
    reliable_partition_store:status(WorkerRef, Timeout).



%% -----------------------------------------------------------------------------
%% @doc Returns true if the process has a workflow context.
%% See {@link workflow/2}.
%% @end
%% -----------------------------------------------------------------------------
-spec is_in_workflow() -> boolean().

is_in_workflow() ->
    get(?WORKFLOW_ID) =/= undefined.


%% -----------------------------------------------------------------------------
%% @doc Fails with a `no_workflow' exception if the calling process doe not
%% have a workflow initiated.
%% @end
%% -----------------------------------------------------------------------------
-spec ensure_in_workflow() -> ok | no_return().

ensure_in_workflow() ->
    is_in_workflow() orelse error(no_workflow),
    ok.


%% -----------------------------------------------------------------------------
%% @doc Returns the current worflow nesting level.
%% Fails with a `no_workflow' exception if this function is not called within
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec workflow_nesting_level() -> pos_integer() | no_return().

workflow_nesting_level() ->
    ok = ensure_in_workflow(),
    get(?WORKFLOW_LEVEL).


%% -----------------------------------------------------------------------------
%% @doc Returns true if the current workflow is nested i.e. has a parent
%% workflow.
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec is_nested_workflow() -> boolean() | no_return().

is_nested_workflow() ->
    ok = ensure_in_workflow(),
    get(?WORKFLOW_LEVEL) > 1.


%% -----------------------------------------------------------------------------
%% @doc Returns the workflow identifier or undefined if there is no workflow
%% initiated for the calling process.
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec workflow_id() -> binary() | no_return().

workflow_id() ->
    Id = get(?WORKFLOW_ID),
    Id =/= undefined orelse error(no_workflow),
    Id.


%% -----------------------------------------------------------------------------
%% @doc When called within the functional object in {@link workflow/2},
%% makes the workflow silently return the tuple {aborted, Reason} as the
%% error reason.
%%
%% Termination of a Babel workflow means that an exception is thrown to an
%% enclosing catch. Thus, the expression `catch babel:abort(foo)' does not
%% terminate the workflow.
%% @end
%% -----------------------------------------------------------------------------
-spec abort(Reason :: any()) -> no_return().

abort(Reason) ->
    throw({aborted, Reason}).



%% -----------------------------------------------------------------------------
%% @doc Equivalent to calling {@link workflow/2} with and empty map passed as
%% the `Opts' argument.
%%
%% > Notice subscriptions are not working at the moment
%% > See {@link yield/1,2} to track progress.
%%
%% @end
%% -----------------------------------------------------------------------------
-spec workflow(Fun :: wf_fun()) ->
    {true | false, wf_result()}
    | {error, Reason :: any()}
    | no_return().

workflow(Fun) ->
    workflow(Fun, #{}).


%% -----------------------------------------------------------------------------
%% @doc Executes the functional object `Fun' as a Reliable workflow, i.e.
%% ordering and scheduling all resulting Riak KV object writes and deletes.
%%
%% Any function that executes inside the workflow that wants to be able to
%% schedule work to Riak KV, needs to use the infrastructure provided in this
%% module to add workflow items to the workflow stack
%% (see {@link add_workflow_items/1}) and to add the precedence amongst them
%% (see {@link add_workflow_precedence/2}).
%%
%% Any other operation, including reading and writing from/to Riak KV by
%% directly using the Riak Client library will work as normal and
%% will not affect the workflow. Only by calling the special functions in this
%% module you can add work items to the workflow.
%%
%% If something goes wrong inside the workflow as a result of a user
%% error or general exception, the entire workflow is terminated and the
%% function raises an exception. In case of an internal error, the function
%% returns the tuple `{error, Reason}'.
%%
%% If everything goes well, the function returns the tuple
%% `{Flag, Result}}' where `Flag' is a boolean denoting whether a workflow was
%% scheduled or not, and Result is a `reliable:wf_result()' structure.
%%
%% > Notice that calling this function schedules the work to Reliable, you need
%% to use the WorkId to check with Reliable the status of the workflow
%% execution.
%%
%% The resulting workflow execution will schedule the writes and deletes in the
%% order defined by the dependency graph constructed using
%% {@link add_workflow_precedence/2}.
%%
%% > If you want to manually determine the execution order of the workflow you
%% should use the {@link enqueue/2} function instead.
%%
%% The `Opts' argument offers the following options:
%%
%% * `on_terminate` – a functional object `fun((Reason :: any()) -> ok)'. This
%% function will be evaluated before the call terminates. In case of successful
%% termination the value `normal' will be  passed as argument. Otherwise, in
%% case of error, the error reason will be passed as argument. This allows you
%% to perform a cleanup after the workflow execution e.g. returning a Riak
%% connection object to a pool. Notice that this function might be called
%% multiple times in the case of nested workflows. If you need to conditionally
%% perform a cleanup operation within the functional object only at the end of
%% the workflow call, you can use the function `is_nested_workflow/0'
%% to make a decision.
%%
%% Calls to this function can be nested and the result is exactly the same as it
%% would without a nested call i.e. nesting workflows does not provide any kind
%% of stratification and thus there is no implicit precedence relationship
%% between workflow items scheduled at different nesting levels, unless you
%% explecitly create those relationships by using the
%% {@link add_workflow_precedence/2} function.

%% ?> **Note on Nested workflows**
%% ?> No final scheduling will be done until the top level workflow is
%% terminated. So, although a nested worflow returns `{true, Result}', if the
%% enclosing parent workflow is aborted, the entire nested workflow is aborted.
%%
%% > Notice subscriptions are not working at the moment
%% > See {@link yield/2} to track progress.
%%
%% @end
%% -----------------------------------------------------------------------------
-spec workflow(Fun :: wf_fun(), Opts :: wf_opts()) ->
    {true | false, wf_result()}
    | {error, Reason :: any()}
    | no_return().

workflow(Fun, Opts) when is_function(Fun, 0) ->
    ok = init_workflow(Opts),
    try
        %% Fun should use this module functions which are workflow aware.
        Result = Fun(),
        case maybe_enqueue_workflow(Opts) of
            {top, true, WorkRef} ->
                ok = on_terminate(normal, Opts),
                {true, worflow_result(Result, WorkRef)};
            {top, false} ->
                ok = on_terminate(normal, Opts),
                {false, worflow_result(Result)};
            {nested, Boolean} ->
                ok = on_terminate(normal, Opts),
                {Boolean, worflow_result(Result)}
        end
    catch
        throw:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Error while executing workflow",
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok = on_terminate(Reason, Opts),
            throw_or_return(Reason);
        _:Reason:Stacktrace ->
            %% A user exception, we need to raise it again up the
            %% nested transation stack and out
            ?LOG_ERROR(#{
                message => "Error while executing workflow",
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok = on_terminate(Reason, Opts),
            error(Reason, Stacktrace)
    after
        ok = maybe_cleanup()
    end.


%% -----------------------------------------------------------------------------
%% @doc Adds a workflow item to the workflow stack.
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec add_workflow_items([wf_item()]) -> ok | no_return().

add_workflow_items(L) ->
    ok = ensure_in_workflow(),
    G = get(?WORKFLOW_GRAPH),
    {Ids, WorkItem} = lists:unzip(L),
    _ = put(?WORKFLOW_GRAPH, reliable_digraph:add_vertices(G, Ids, WorkItem)),
    ok.

%% -----------------------------------------------------------------------------
%% @doc Returns a workflow item that was previously added to the workflow stack
%% with the {@link add_workflow_items/2} function.
%% Fails with a `badkey' exception if there is no workflow item identified by
%% `Id'.
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
- spec get_workflow_item(wf_item_id()) ->
    wf_item() | no_return() | no_return().

get_workflow_item(Id) ->
    ok = ensure_in_workflow(),
    case reliable_digraph:vertex(get(?WORKFLOW_GRAPH), Id) of
        {Id, _} = Item ->
            Item;
        false ->
            error(badkey)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec find_workflow_item(wf_item_id()) ->
    {ok, wf_item()} | error | no_return().

find_workflow_item(Id) ->
    ok = ensure_in_workflow(),
    case reliable_digraph:vertex(get(?WORKFLOW_GRAPH), Id) of
        {Id, _} = Item ->
            {ok, Item};
        false ->
            error
    end.


%% -----------------------------------------------------------------------------
%% @doc Relates on or more workflow items in a precedence relationship. This
%% relationship is used by the {@link workflow/2} function to determine the
%% workflow execution order based on the resulting precedence graph topsort
%% calculation.
%% Fails with a `no_workflow' exception if this function is not called withihn
%% a worflow context.
%% @end
%% -----------------------------------------------------------------------------
-spec add_workflow_precedence(
    As :: wf_item_id() | [wf_item_id()],
    Bs :: wf_item_id() | [wf_item_id()]) -> ok | no_return().

add_workflow_precedence(As, Bs) when is_list(As) andalso is_list(Bs) ->
    ok = ensure_in_workflow(),
    G0 = get(?WORKFLOW_GRAPH),
    Comb = [{A, B} || A <- As, B <- Bs],
    G1 = lists:foldl(
        fun({A, B}, G) -> reliable_digraph:add_edge(G, A, B) end,
        G0,
        Comb
    ),
    _ = put(?WORKFLOW_GRAPH, G1),
    ok;

add_workflow_precedence(As, B) when is_list(As) ->
    add_workflow_precedence(As, [B]);

add_workflow_precedence(A, Bs) when is_list(Bs) ->
    add_workflow_precedence([A], Bs);

add_workflow_precedence(A, B) ->
    add_workflow_precedence([A], [B]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_workflow_event_payload() -> Term :: any() | undefined.

get_workflow_event_payload() ->
    get(?WORKFLOW_EVENT_PAYLOAD).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_workflow_event_payload(Term :: any() | undefined) -> ok.

set_workflow_event_payload(Term) ->
    _ = put(?WORKFLOW_EVENT_PAYLOAD, Term),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init_workflow(Opts) ->
    case get(?WORKFLOW_ID) of
        undefined ->
            %% We are initiating a new top workflow
            %% We store the worflow state in the process dictionary
            Id = get_work_id(Opts),
            undefined = put(?WORKFLOW_ID, Id),
            undefined = put(?WORKFLOW_LEVEL, 1),
            undefined = put(?WORKFLOW_GRAPH, reliable_digraph:new()),
            ok;
        _Id ->
            %% This is a nested call, we are joining an existing workflow
            ok = increment_nesting_level(),
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
worflow_result(Result) ->
    worflow_result(Result, undefined).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
worflow_result(Result, Ref) when is_tuple(Ref) orelse Ref == undefined ->
    #{
        work_id => get(?WORKFLOW_ID),
        work_ref => Ref,
        result => Result,
        is_nested => is_nested_workflow()
    }.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
increment_nesting_level() ->
    N = get(?WORKFLOW_LEVEL),
    N = put(?WORKFLOW_LEVEL, N + 1),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
decrement_nested_count() ->
    N = get(?WORKFLOW_LEVEL),
    N = put(?WORKFLOW_LEVEL, N - 1),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec maybe_enqueue_workflow(Opts :: map()) ->
    {top, true, WorkRef :: reliable_work_ref:t()}
    | {top | nested, boolean()}
    | no_return().

maybe_enqueue_workflow(Opts) ->
    case is_nested_workflow() of
        true ->
            %% TODO We should be returning false if there where no tasks
            %% scheduled at this nesting level BUT we currently do not track
            %% tasks by level, so we cannot tell right now.
            {nested, true};
        false ->
            enqueue_workflow(Opts)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue_workflow(Opts :: map()) ->
    {top, true, WorkRef :: reliable_work_ref:t()}
    | {top, false}
    | no_return().

enqueue_workflow(Opts) ->
    case prepare_tasks() of
        {ok, []} ->
            {top, false};
        {ok, Tasks} ->
            NewOpts = Opts#{
                work_id => get(?WORKFLOW_ID),
                event_payload => get_workflow_event_payload()
            },
            case enqueue(Tasks, NewOpts) of
                {ok, WorkRef} ->
                    {top, true, WorkRef};
                {error, Reason} ->
                    throw(Reason)
            end;
        {error, Reason} ->
            throw(Reason)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prepare_tasks() ->
    G = get(?WORKFLOW_GRAPH),
    case reliable_digraph:topsort(G) of
        false ->
            {error, no_work};
        Vertices ->
            prepare_tasks(Vertices, G, 1, [])
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prepare_tasks([], _, _, Acc) ->
    {ok, lists:reverse(Acc)};

prepare_tasks([H|T], G, SeqNum0, Acc0) ->
    {_Id, Action} = reliable_digraph:vertex(G, H),
    {SeqNum, Acc1} = case action_task(Action) of
        undefined ->
            {SeqNum0, Acc0};
        Task ->
            {SeqNum0 + 1, [{SeqNum0, Task}|Acc0]}
    end,
    prepare_tasks(T, G, SeqNum, Acc1).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
action_task(undefined) ->
    undefined;

action_task({_, Fun}) when is_function(Fun, 0) ->
    Fun();

action_task({_, Task}) ->
    case reliable_task:is_type(Task) of
        true -> Task;
        false -> error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_cleanup() ->
    ok = decrement_nested_count(),
    case get(?WORKFLOW_LEVEL) == 0 of
        true ->
            cleanup();
        false ->
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
cleanup() ->
    %% We cleanup the process dictionary
    _ = erase(?WORKFLOW_ID),
    _ = erase(?WORKFLOW_LEVEL),
    _ = erase(?WORKFLOW_GRAPH),
    _ = erase(?WORKFLOW_EVENT_PAYLOAD),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec throw_or_return(Reason :: any()) -> no_return() | {error, any()}.

throw_or_return(Reason) ->
    case is_nested_workflow() of
        true ->
            throw(Reason);
        false ->
            {error, Reason}
    end.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
on_terminate(Reason, #{on_terminate := Fun}) when is_function(Fun, 1) ->
    _ = Fun(Reason),
    ok;

on_terminate(_, _) ->
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Gets Id from opts or generates a ksuid
%% @end
%% -----------------------------------------------------------------------------
get_work_id(#{work_id := Id}) when is_binary(Id) ->
    Id;

get_work_id(#{work_id := Id}) ->
    error({badarg, {work_id, Id}});

get_work_id(_) ->
    ksuid:gen_id(millisecond).


%% @private
maybe_subscribe(_WorkRef, _Opts) ->
    %% This process can only subscribe to one event, otherwise we get an
    %% exception from gproc
    %% TODO not supported at the moment till we have distributed pubsub
    false.


%% @private
unsubscribe(_WorkRef) ->
    ok.


%% @private
-spec yield(
    WorkRef :: reliable_work_ref:t(),
    Timeout :: integer(),
    Status :: undefined | reliable_work:status()) ->
    {ok, Payload :: any()} | {error, any()} | timeout.


yield(_, 0, _) ->
    timeout;

yield(WorkRef, Timeout, Status0) ->
    SleepTime = 1000,
    T0 = erlang:system_time(millisecond),

    case status(WorkRef) of
        {in_progress, Status1} ->
            ok = yield_sleep(SleepTime, Timeout),
            T1 = erlang:system_time(millisecond),
            yield(WorkRef, Timeout - (T1 - T0), Status1);

        {failed, Status1} ->
            {ok, maps:get(event_payload, Status1)};

        {error, not_found} = Error when Status0 == undefined ->
            Error;

        {error, not_found} ->
            %% At the moment a completed or failed Work is removed
            %% so the absence is regarded as completion
            %% TODO this will change in the future as we will retain work
            %% objects
            {ok, maps:get(event_payload, Status0)};

        {error, Reason} ->
            error(Reason)
    end.


%% @private
yield_sleep(Time, Timeout) when Timeout =< Time ->
    ok;

yield_sleep(Time, _) ->
    timer:sleep(Time).
