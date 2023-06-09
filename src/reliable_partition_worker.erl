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

-define(LIST_MAX_RESULTS, 100).

-record(state, {
    %% Queue storage
    queue_ref               ::  atom(),
    %% Partition
    bucket                  ::  binary(),
    %% Variable substitution
    symbolics = #{}         ::  map(),
    %% Works not executed yet
    pending = []            ::  [reliable_work:t()],
    %% Works executed but not yet acked to queue
    pending_acks = []       ::  [{status(), reliable_work:t()}],
    %% Current work state
    work_system_time        ::  optional(pos_integer()),
    work_start_time         ::  optional(integer()),
    work_retry              ::  reliable_retry:t(),
    work_retry_tref         ::  reference() | undefined,
    %% Current task state
    task_system_time        ::  optional(pos_integer()),
    task_start_time         ::  optional(integer()),
    task_retry              ::  reliable_retry:t(),
    task_count = 0          ::  non_neg_integer()
}).

-type state()               ::  #state{}.
-type status()              ::  completed | failed.
-type optional(T)           ::  T | undefined.

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
    ok = logger:set_process_metadata(#{reliable_partition => Bucket}),

    ?LOG_DEBUG("Initializing partition worker"),

    State = #state{
        queue_ref = QueueRef,
        bucket = Bucket,
        work_retry = new_work_retry(),
        task_retry = new_task_retry()
    },

    %% We use handle_continue to loop while using exponential backoff.
    %% The first thing we need to do is obtain a connection to Riak KV.
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
    %% This is the connection we use to perform the tasks in reliable_work.
    %% If we cannot obtain a connection this function will fail forcing this
    %% server to crash here.
    Conn = get_riak_connection(),

    %% reliable_task(s) are MFAs denoting operations with Riak KV. The first
    %% argument in those operations i the Riak KV connection which is opnly
    %% known at runtime, so tasks use a form of variable (coined "symbolics" by
    %% Chris) that we need to replace when performing the task.
    %% The symbolics map is used for variable substitution used by
    %% replace_symbolics/2.
    State = State0#state{
        symbolics = #{riakc => Conn}
    },
    continue({backoff, normal}, State);

handle_continue({backoff, Reason}, State0) ->
    try
        State = backoff(Reason, State0),
        %% It is important to hibernate here, so that we trigger GC.
        %% This is nice because chances are we are going to be doing
        %% nothing till the next 'work' timeout signal is received.
        %% check handle_info/2.
        {noreply, State, hibernate}
    catch
        error:EReason when EReason == deadline; EReason == max_retries ->
            %% We hit the retry limit, so we stop. This will trigger the
            %% supervisor to restart the worker, so we have the opportunity to
            %% resolve any environmental issues affecting the connection with
            %% the queue which is causing the operations to fail.

            ok = notify_work_exception(error, EReason, [], State0),
            {stop, EReason, State0}
    end;

handle_continue(work, #state{pending_acks = [{Status, Work} | Rest]} = State) ->
    %% We have work that we need to acknowledge to queue. This is both when the
    %% work was completed and when it failed. In both cases the work object will
    %% be removed from the queue. In the case of a failed work it will me
    %% published to the DLQ.
    %% We will retry this operation till it works or till we hit the
    %% max_retries or deadline limits.
    %% This means no new work object is considered till we succeed in
    %% acknowledging the previous one (pending_acks is a list but at the moment
    %% it can contain at most one element).
    QueueRef = State#state.queue_ref,
    WorkId = reliable_work:id(Work),

    case ack(Status, QueueRef, Work) of
        ok ->
            ?LOG_INFO(#{
                description => "Work acknowledged to queue.",
                work_status => Status,
                work_id => WorkId
            }),
            ok = notify_work_stop(Status, Work, State),
            continue(work, State#state{pending_acks = Rest});

        {error, Reason} ->
            ?LOG_ERROR(#{
                description =>
                    "Failed to acknowledge work to queue. "
                    "Retrying using exponential backoff.",
                reason => Reason,
                work_id => WorkId
            }),
            continue({backoff, Reason}, State)
    end;

handle_continue(work, #state{pending = [Work0 | Rest]} = State0) ->
    %% We have no more elements in pending_acks but we do have some additional
    %% work objects we obtained from a previous ask.
    %% This will go through each of the work's tasks, executing them using a
    %% separate exponential backoff (task_retry). That is, the worker will try
    %% to execute the tasks until completed or
    %% failed.
    case handle_work(Work0, State0) of
        {ok, {Status, _Work} = Result, State1}
        when Status == completed; Status == failed ->
            State = State1#state{
                pending_acks = [Result | State0#state.pending_acks],
                pending = Rest
            },
            %% We do not notify here as we need to ack the work to the queue
            continue(work, State);

        {error, Reason, _Work, State} ->
            %% This is a temporary failure (lost riak connection,
            %% connection timeout, overload, etc), so we'll retry the same work
            %% object using exponential backoff. Thus, we do not notify.
            continue({backoff, Reason}, State)
    end;

handle_continue(work, #state{pending = []} = State) ->
    %% Cleanup logger metadata
    ok = logger:set_process_metadata(#{
        reliable_partition => State#state.bucket
    }),

    %% Nothing else pending, ask for more work from partition queue (store)
    Opts = #{max_results => ?LIST_MAX_RESULTS},

    case ask(State#state.queue_ref, Opts) of
        {ok, []} ->
            continue({backoff, normal}, State);

        {ok, L} ->
            continue(work, reset_backoff(State#state{pending = L}));

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


handle_info({timeout, Ref, work}, #state{work_retry_tref = Ref} = State) ->
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
new_work_retry() ->
    Tag = work, % fired timeout signal will use this Tag
    Config = reliable_config:get(worker_retry),
    Type = maps:get(backoff_type, Config, jitter),
    Floor = maps:get(backoff_min, Config, timer:seconds(1)),
    Ceiling = maps:get(backoff_max, Config, timer:minutes(1)),
    Deadline = maps:get(deadline, Config, 0), % disabled
    MaxRetries = maps:get(max_retries, Config, 0), % disabled


    is_integer(Floor) andalso is_integer(Ceiling) andalso Floor < Ceiling
        orelse error(badarg),

    reliable_retry:init(Tag, #{
        backoff_enabled => true,
        deadline => Deadline,
        max_retries => MaxRetries,
        backoff_type => Type,
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
        work_system_time = erlang:system_time(),
        work_start_time = erlang:monotonic_time(),
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
reset_backoff(#state{work_retry = R0} = State) ->
    %% We reset the retry strategy
    {_, R1} = reliable_retry:succeed(R0),
    State#state{work_retry = R1}.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
backoff(normal, #state{work_retry = R0} = State) ->
    %% We reset the retry strategy
    {Delay, R1} = reliable_retry:succeed(R0),

    ?LOG_DEBUG(#{
        description => "Work scheduled",
        delay => Delay
    }),

    fire_backoff(R1, State);

backoff(Reason, #state{work_retry = R0} = State) ->
    {Delay, R1} = reliable_retry:fail(R0),

    ?LOG_DEBUG(#{
        description => "Work scheduled with incremental backoff",
        reason => Reason,
        delay => Delay
    }),

    fire_backoff(R1, State).


%% @private
fire_backoff(Retry, State) ->
    State#state{
        work_retry = Retry,
        work_retry_tref = reliable_retry:fire(Retry)
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
    {ok, {status(), reliable_work:t()}, state()}
    | {error, Reason :: any(), reliable_work:t(), state()}.

handle_work(Work, State0) ->
    %% Start executing work (this might be a work item we have seen before we
    %% crashed/shutdown and restarted or a brand new one).

    State = reset_state(State0),

    ok = logger:update_process_metadata(#{
        reliable_work_id => reliable_work:id(Work)
    }),

    ok = notify_work_start(Work, State),

    try
        handle_work(reliable_work:tasks(Work), Work, State)
        %% We do not notify exception here as we need to ack the work to the
        %% queue
    catch
        throw:Reason ->
            %% We do not notify exception here as we might retry
            {error, Reason, Work, State}
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
    {ok, {status(), reliable_work:t()}, state()}
    | {error, Reason :: any(), reliable_work:t(), state()}.

handle_work([{TaskId, Task0}|T], Work0, State0) ->
    Status = reliable_task:status(Task0),

    ?LOG_DEBUG(#{
        description => "Found task to be performed.",
        task_id => TaskId,
        task => Task0,
        partition => State0#state.bucket,
        status => Status
    }),

    case Status of
        completed ->
            %% Skip task
            handle_work(T, Work0, State0);

        Status when Status == undefined; Status == failed ->
            %% handle_task/4 will retry the task infinitely only returning an
            %% error in case there is a permanent failure.
            State1 = State0#state{
                task_start_time = erlang:monotonic_time(),
                task_system_time = erlang:system_time()
            },
            ok = notify_task_start(TaskId, Work0, State1),

            case handle_task(TaskId, Task0, Work0, State1) of
                {completed, Work} ->
                    %% We store the work using a flow control mechanism.
                    State = State1#state{
                        task_count = State1#state.task_count + 1
                    },
                    ok = maybe_store_work(Work, State),
                    ok = notify_task_stop(completed, TaskId, Work, State),

                    %% We continue with next task.
                    handle_work(T, Work, State);

                {failed, Work} = Result ->
                    %% We stop
                    ok = notify_task_stop(failed, TaskId, Work, State1),
                    {ok, Result, State1}
            end
    end;

handle_work([], Work, State) ->
    {ok, {completed, Work}, State}.


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
        reliable_task_id => TaskId
    }),

    ?LOG_DEBUG(#{
        description => "Trying to perform task",
        attempts => reliable_retry:count(State#state.task_retry)
    }),

    try apply_task(Task0, State) of
        {error, Reason} when Reason =/= unmodified ->
            throw(Reason);
        Res ->
            %% Res includes {error, unmodified} which is a non-error return by
            %% Riak when a Riak Datatype update operation has noop.
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
            ?LOG_DEBUG(#{description => "Updating work in queue delayed"}),
            ok
    end.


%% @private
store_work(QueueRef, Work) ->
    case reliable_partition_store:update(QueueRef, Work) of
        ok ->
            ?LOG_DEBUG(#{description => "Updated work in queue"}),
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR(#{
                description => "Failed to update work in queue",
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
    %% We move the workd to DLQ
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
notify_work_start(Work, State) ->
    telemetry:execute(
        [reliable, work, execute, start],
        #{
            monotonic_time => State#state.work_start_time,
            system_time => State#state.work_system_time
        },
        #{
            work_ref => reliable_work:ref(State#state.queue_ref, Work),
            partition => State#state.bucket
        }
    ).

%% @private
notify_work_stop(Status, Work, State) ->
    StopTime = erlang:monotonic_time(),
    StartTime = State#state.work_start_time,
    WorkRef = reliable_work:ref(State#state.queue_ref, Work),
    Payload = reliable_work:event_payload(Work),

    %% Notify subscribers
    Event = {
        reliable_event,
        #{
            status => Status,
            work_ref => WorkRef,
            payload => Payload
        }
    },

    ok = reliable_event_manager:notify(Event),

    %% Emmit telemetry event
    telemetry:execute(
        [reliable, work, execute, stop],
        #{
            count => 1,
            duration => StopTime - StartTime,
            monotonic_time => StopTime,
            retries => reliable_retry:count(State#state.work_retry)
        },
        #{
            work_ref => WorkRef,
            status => Status,
            partition => State#state.bucket
        }
    ).


%% @private
notify_work_exception(Class, Reason, Stacktrace, State) ->
    %% We must have at least once, otherwise we wouldn't be called
    [{Status, Work} | _] = State#state.pending_acks,
    StopTime = erlang:monotonic_time(),
    StartTime = State#state.work_start_time,
    WorkRef = reliable_work:ref(State#state.queue_ref, Work),

    %% Emmit telemetry event


    telemetry:execute(
        [reliable, work, execute, exception],
        #{
            count => 1,
            duration => StopTime - StartTime,
            monotonic_time => StopTime,
            retries => reliable_retry:count(State#state.work_retry)
        },
        #{
            class => Class,
            reason => Reason,
            stacktrace => Stacktrace,
            work_ref => WorkRef,
            status => Status,
            partition => State#state.bucket
        }
    ).


%% @private
notify_task_start(TaskId, Work, State) ->
    telemetry:execute(
        [reliable, task, execute, start],
        #{
            monotonic_time => State#state.task_start_time,
            system_time => State#state.task_system_time
        },
        #{
            task_id => TaskId,
            work_ref => reliable_work:ref(State#state.queue_ref, Work),
            partition => State#state.bucket
        }
    ).


%% @private
notify_task_stop(Status, TaskId, Work, State) ->
    StopTime = erlang:monotonic_time(),
    StartTime = State#state.task_start_time,
    WorkRef = reliable_work:ref(State#state.queue_ref, Work),

    telemetry:execute(
        [reliable, task, execute, stop],
        #{
            count => 1,
            duration => StopTime - StartTime,
            monotonic_time => StopTime,
            retries => reliable_retry:count(State#state.task_retry)
        },
        #{
            task_id => TaskId,
            work_ref => WorkRef,
            status => Status,
            partition => State#state.bucket
        }
    ).


%% @private
%% notify_task_exception(Class, Reason, Stacktrace, TaskId, Work, State) ->
%%     %% We must have at least once, otherwise we wouldn't be called
%%     [{Status, Work} | _] = State#state.pending_acks,
%%     StopTime = erlang:monotonic_time(),
%%     StartTime = State#state.work_start_time,
%%     WorkRef = reliable_work:ref(State#state.queue_ref, Work),

%%     %% Emmit telemetry event


%%     telemetry:execute(
%%         [reliable, task, execute, exception],
%%         #{
%%             count => 1,
%%             duration => StopTime - StartTime,
%%             monotonic_time => StopTime,
%%             retries => reliable_retry:count(State#state.work_retry)
%%         },
%%         #{
%%             class => Class,
%%             reason => Reason,
%%             stacktrace => Stacktrace,
%%             task_id => TaskId,
%%             work_ref => WorkRef,
%%             status => Status,
%%             partition => State#state.bucket
%%         }
%%     ).




