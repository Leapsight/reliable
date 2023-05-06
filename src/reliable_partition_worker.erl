%% =============================================================================
%%  reliable_partition_worker.erl -
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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(reliable_partition_worker).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-define(LIST_MAX_RESULTS, 10).

-record(state, {
    queue_ref               ::  atom(),
    bucket                  ::  binary(),
    symbolics = #{}         ::  map(),
    pending = []            ::  [reliable_work:t()],
    pending_acks = []       ::  [{status(), reliable_work:t()}],
    retry                   ::  reliable_retry:t(),
    retry_tref              ::  reference() | undefined,
    task_retry              ::  reliable_retry:t(),
    task_count = 0          ::  non_neg_integer()
}).

-type state()               ::  #state{}.
-type status()              ::  completed | failed.

%% API
-export([start_link/3]).

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% =============================================================================
%% API
%% =============================================================================


-spec start_link(
    WorkerName :: atom(), QueueRef :: atom(), Bucket :: binary()) ->
    {ok, pid()} | ignore | {error, any()}.

start_link(WorkerName, QueueRef, Bucket) ->
    gen_server:start_link({local, WorkerName}, ?MODULE, [QueueRef, Bucket], []).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([QueueRef, Bucket]) ->
    ok = logger:update_process_metadata(#{partition => Bucket}),

    ?LOG_DEBUG("Initializing partition store"),

    State = #state{
        queue_ref = QueueRef,
        bucket = Bucket,
        retry = new_retry(),
        task_retry = new_task_retry()
    },

    {ok, State, {continue, connect}}.


%% -----------------------------------------------------------------------------
%% @doc
%% Accepts the following commands:
%%
%% - `connect': obtains a connection to Riak KV (there is no reconnect as we
%% use the Riak client auto_reconnect option).
%% - `{backoff, normal | any()}'
%% - `work'
%% @end
%% -----------------------------------------------------------------------------
handle_continue(connect, State0) ->
    %% This the connection we will use to process the work
    Conn = get_riak_connection(),

    State = State0#state{
        symbolics = #{riakc => Conn}
    },
    continue({backoff, normal}, State);

handle_continue({backoff, Reason}, State0) ->
    try
        State = backoff(Reason, State0),
        %% It is important to hibernate here, so that we trigger GC.
        %% This is nice because chances are we are going to be doing
        %% nothing till the the next 'work' signal is received.
        {noreply, State, hibernate}
    catch
        error:Reason when Reason == deadline; Reason == max_retries ->
            {stop, Reason, State0}
    end;

handle_continue(work, #state{pending_acks = [{Status, Work} | Rest]} = State) ->
    QueueRef = State#state.queue_ref,
    WorkId = reliable_work:id(Work),

    case ack(Status, QueueRef, Work) of
        ok ->
            ?LOG_INFO(#{
                description => "Work acknowledged to queue.",
                status => Status,
                work_id => WorkId
            }),
            ok = notify(QueueRef, Status, Work),
            continue(work, State#state{pending_acks = Rest});

        {error, Reason} ->
            ?LOG_ERROR(#{
                description => "Failed to acknowledge work to queue. Retrying.",
                reason => Reason,
                work_id => WorkId
            }),
            continue({backoff, Reason}, State)
    end;

handle_continue(work, #state{pending = [Work | Rest]} = State0) ->
    case handle_work(Work, State0) of
        {Status, _} = Result when Status == completed; Status == failed ->
            State = State0#state{
                pending_acks = [Result | State0#state.pending_acks],
                pending = Rest
            },
            continue(work, State);

        {error, Reason, Work} ->
            %% This is a temporary failure (lost riak connection, timeout,
            %% overload, etc), so we retry the same work next using
            %% exponential backoff.
            continue({backoff, Reason}, State0)
    end;

handle_continue(work, #state{pending = []} = State) ->
    %% Nothing else pending, ask for more work from partition queue (store)
    Opts = #{max_results => ?LIST_MAX_RESULTS},

    case ask(State#state.queue_ref, Opts) of
        {ok, L} ->
            continue(work, State#state{pending = L});

        {error, Reason} ->
            ?LOG_WARNING(#{
                description => "Failed when listing work. Nothing done.",
                reason => Reason
            }),

            continue({backoff, Reason}, State)
    end;

handle_continue(Event, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled event",
        event => Event
    }),
    {noreply, State}.


handle_call(Event, From, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled call",
        event => Event,
        from => From
    }),
    {reply, {error, not_implemented}, State}.


handle_cast(Event, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled cast",
        event => Event
    }),
    {noreply, State}.


handle_info({timeout, Ref, work}, #state{retry_tref = Ref} = State) ->
    %% Do some work
    continue(work, State);

handle_info(Event, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled info",
        reason => Event
    }),
    {noreply, State}.


terminate(_Reason, _State) ->
    ?LOG_DEBUG("Terminating."),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
new_retry() ->
    Tag = work, %% fired timeout signal will use this Tag

    Floor = reliable_config:get(pull_backoff_min, timer:seconds(2)),
    Ceiling = reliable_config:get(pull_backoff_max, timer:minutes(2)),

    is_integer(Floor) andalso is_integer(Ceiling) orelse error(badarg),

    reliable_retry:init(Tag, #{
        deadline => timer:minutes(10),
        max_retries => 100,
        backoff_enabled => true,
        backoff_type => jitter,
        backoff_min => Floor,
        backoff_max => Ceiling
    }).


%% @private
new_task_retry() ->
    reliable_retry:init(task, #{
        deadline => 0, % infinity
        max_retries => 0, % infinity
        backoff_enabled => true,
        backoff_type => jitter,
        backoff_min => 500,
        backoff_max => timer:minutes(15)
    }).



%% @private
reset_state(State) ->

    State#state{
        task_retry = new_task_retry(),
        task_count = 0
    }.


%% @private
get_riak_connection() ->
    Host = reliable_config:riak_host(),
    Port = reliable_config:riak_port(),
    %% If riakc gets disconnected dureing an operation we'll get
    %% {error, disconnected} but we'll treat it as a temporal failure
    %% as we are asking riakc to automatically reconnect.
    Opts = [
        {queue_if_disconnected, false},
        {auto_reconnect, true},
        {keepalive, true}
    ],

    %% We link to the connection, so if it crashes we will crash too and be
    %% restarted by reliable_partition_store_sup
    {ok, Conn} = riakc_pb_socket:start_link(Host, Port, Opts),

    %% We verify the connection works, otherwise we crash
    %% pong = riakc_pb_socket:ping(Conn),

    ?LOG_DEBUG(#{description => "Got connection to Riak"}),

    Conn.


%% @private
continue(Cmd, State) ->
    {noreply, State, {continue, Cmd}}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
backoff(normal, #state{retry = R0} = State) ->
    %% We reset the retry strategy
    {Delay, R1} = reliable_retry:succeed(R0),

    ?LOG_DEBUG(#{
        description => "Work scheduled",
        delay => Delay
    }),

    fire_backoff(R1, State);

backoff(Reason, #state{retry = R0} = State) ->
    {Delay, R1} = reliable_retry:fail(R0),

    ?LOG_DEBUG(#{
        description => "Work scheduled with incremented backoff",
        reason => Reason,
        delay => Delay
    }),

    fire_backoff(R1, State).


%% @private
fire_backoff(Retry, State) ->
    State#state{
        retry = Retry,
        retry_tref = reliable_retry:fire(Retry)
    }.



%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns the next batch of work items from the partition queue (store).
%% @end
%% -----------------------------------------------------------------------------
ask(QueueRef, Opts) ->
    ?LOG_DEBUG(#{description => "Fetching work"}),

    case reliable_partition_store:list(QueueRef, Opts) of
        {ok, {WorkList, _Cont}} ->
            %% We ignore the continuation, we simply query again on the next
            %% scheduled run.
            {ok, WorkList};

        {error, _} = Error ->
            Error
    end.


%% @private
-spec handle_work(reliable_work:t(), state()) ->
    {status(), reliable_work:t()}
    | {error, Reason :: any(), reliable_work:t()}.

handle_work(Work, State0) ->
    WorkId = reliable_work:id(Work),
    Payload = reliable_work:event_payload(Work),
    N = reliable_work:nbr_of_tasks(Work),

    ok = logger:update_process_metadata(#{
        work => #{
            id => WorkId,
            event_payload => Payload,
            nbr_of_tasks => N
        }
    }),

    State = reset_state(State0),

    try
        handle_work(reliable_work:tasks(Work), Work, State)
    catch
        throw:Reason ->
            {error, Reason, Work}
    end.


%% -----------------------------------------------------------------------------
%% @private
%% If a task fails and the failure is temporary (transient or intermittent),
%% it will keep on retrying using exponential backoff and thus
%% not returning.
%%
%% This is under the assumption that transient and intermittent failures are
%% Riak KV related (as currently Reliable is designed to perform Riak KV
%% operations only - even if the API allows tasks to be anything).
%%
%% If the failure is permanent, teh function will return `{error, Reason}'.
%% @end
%% -----------------------------------------------------------------------------
-spec handle_work(
    [{pos_integer(), reliable_task:t()}], reliable_work:t(), state()) ->
    {status(), reliable_work:t()}.

handle_work([{TaskId, Task0}|T], Work0, State0) ->
    ?LOG_DEBUG(#{
        description => "Found task to be performed.",
        task_id => TaskId,
        task => Task0
    }),

    case reliable_task:status(Task0) of
        completed ->
            %% Skip task
            handle_work(T, Work0, State0);

        Status when Status == undefined; Status == failed ->
            %% handle_task/4 will retry the task infinitely only returning an
            %% error in case there is a permanent failure.
            case handle_task(TaskId, Task0, Work0, State0) of
                {completed, Work} ->
                    %% We store the work using a flow control mechanism.
                    State = State0#state{
                        task_count = State0#state.task_count + 1
                    },
                    ok = maybe_store_work(Work, State),

                    %% We continue with next task.
                    handle_work(T, Work, State);

                {failed, _} = Result ->
                    %% We stop
                    Result
            end
    end;

handle_work([], Work, _) ->
    {completed, Work}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc IT is important that here we only perofmr operations with the Riak
%% connection we own and not other operations such as reliable_partition_store
%% as its backend might use Riak itself and we will get a wrong interpreation
%% of the Riak errors.
%% @end
%% -----------------------------------------------------------------------------
-spec handle_task(
    TaskId :: integer(), reliable_task:t(), reliable_work:t(), state()) ->
    {status(), reliable_work:t()}.

handle_task(TaskId, Task0, Work0, State) ->
    ok = logger:update_process_metadata(#{
        task_id => TaskId
    }),

    ?LOG_DEBUG(#{
        description => "Trying to perform task",
        attempts => reliable_retry:count(State#state.task_retry)
    }),

    %% Result = rpc:call(Node, Module, Function, Args),
    try apply_task(Task0, State) of
        {error, Reason} ->
            throw(Reason);
        Res ->
            Task1 = reliable_task:set_result(Res, Task0),
            Task = reliable_task:set_status(completed, Task1),
            Work = reliable_work:update_task(TaskId, Task, Work0),
            {completed, Work}
    catch
        Class:EReason:Stacktrace ->
            Reason = reliable_riak_util:format_error_reason(EReason),
            Task1 = reliable_task:set_result({error, EReason}, Task0),
            Task = reliable_task:set_status(failed, Task1),
            Work = reliable_work:update_task(TaskId, Task, Work0),
            retry_task(
                {Class, Reason, Stacktrace}, TaskId, Task, Work, State
            )
    end.


%% @private
apply_task(Task, #state{} = State) ->
    Module = reliable_task:module(Task),
    Function = reliable_task:function(Task),
    Symbolics = State#state.symbolics,
    Args = replace_symbolics(reliable_task:args(Task), Symbolics),
    erlang:apply(Module, Function, Args).


%% @private
retry_task({Class, datatype_mismatch = Reason, Stacktrace}, _, _, Work, _) ->
    %% Permanent error
    ?LOG_ERROR(#{
        description => "Exception while performing task",
        reason => Reason,
        class => Class,
        stacktrace => Stacktrace
    }),
    {failed, Work};

retry_task(Reason, TaskId, Task, Work, State0)
when
Reason == too_many_fails orelse
Reason == overload orelse
Reason == timeout orelse
Reason == disconnected orelse
is_tuple(Reason) andalso element(1, Reason) == n_val_violation ->
    %% Temporal error, sleep using exponential backoff and retry
    %% We disabled max_retries and deadlines so this call will always return an
    %% integer Delay.
    case reliable_retry:fail(State0#state.task_retry) of
        {Delay, R} when is_integer(Delay) ->
            timer:sleep(Delay),
            State = State0#state{task_retry = R},
            handle_task(TaskId, Task, Work, State);

        {FailReason, _}
        when FailReason == deadline; FailReason == max_retries ->
            %% This should not happen as we are disabling deadlines and
            %% max_retries for tasks
            ?LOG_ERROR(#{
                description => "Failed to complete task.",
                reason => FailReason
            }),
            {failed, Work}
    end;

retry_task(_, _, _, Work, _) ->
    {failed, Work}.


%% @private
maybe_store_work(Work, State) ->
    %% We use log2 as a simple flow control mechanism, to avoid writing to
    %% store for every task update. For example, this will result in:
    %% 1 write when N = 1
    %% 2 writes when N = 5
    %% 3 writes when N = 100
    %% 16 writes when N = 500
    %% 111 writes when N = 1000
    %% Plus the final write when the whole work is completed
    N = reliable_work:nbr_of_tasks(Work),
    Divisor = trunc(math:log2(max(2, N))),



    case State#state.task_count rem Divisor == 0 of
        true ->
            %% We ignore errors here
            _ = store_work(State#state.queue_ref, Work),
            ok;

        false ->
            ?LOG_DEBUG(#{description => "Updating work in store delayed"}),
            ok
    end.


%% @private
store_work(QueueRef, Work) ->
    ?LOG_DEBUG(#{description => "Updating work in store"}),
    case reliable_partition_store:update(QueueRef, Work) of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR(#{
                description => "Failed to update work in store",
                reason => Reason
            }),
            Error
    end.


ack(completed, QueueRef, Work) ->
    %% TODO At the moment we are removing but we should update
    %% instead and let the store backend decide where to store
    %% the completed work so that users can check and report.
    reliable_partition_store:delete(QueueRef, Work);

ack(failed, QueueRef, Work) ->
    reliable_partition_store:move_to_dlq(QueueRef, Work, #{}).


%% -----------------------------------------------------------------------------
%% @private
%% @doc We replace the symbolics in the Task's arguments.
%% Symbolics are variables that have to be replaced at runtime. In most cases
%% this is the variable representing the Riak Connection which is a mandatory
%% argument in all riakc function calls.
%% @end
%% -----------------------------------------------------------------------------
-spec replace_symbolics(list(), map()) -> list().

replace_symbolics(Args, Symbolics) when is_map(Symbolics) ->
    lists:map(
        fun(Arg) ->
            case Arg of
                {symbolic, Key} ->
                    %% We default to the original symbolic value if not found
                    maps:get(Key, Symbolics, Arg);
                _ ->
                    Arg
            end
        end,
        Args
    ).


%% @private
notify(QueueRef, Status, Work) ->
    WorkRef = reliable_work:ref(QueueRef, Work),
    Payload = reliable_work:event_payload(Work),

    Event = {
        reliable_event,
        #{
            status => Status,
            work_ref => WorkRef,
            payload => Payload
        }
    },

    ok = reliable_event_manager:notify(Event).


