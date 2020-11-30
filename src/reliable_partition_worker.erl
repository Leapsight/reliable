%% =============================================================================
%%  reliable_partition_worker.erl -
%%
%%  Copyright (c) 2020 Christopher Meiklejohn. All rights reserved.
%%  Copyright (c) 2020 Leapsight Holdings Limited. All rights reserved.
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

-module(reliable_partition_worker).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").



-record(state, {
    store_ref               ::  atom(),
    bucket                  ::  binary(),
    symbolics               ::  dict:dict(),
    fetch_backoff           ::  backoff:backoff() | undefined,
    fetch_timer_ref         ::  reference() | undefined,
    work_state              ::  work_state() | undefined
}).

-type work_state()          ::  #{
    work            :=  reliable_work:t(),
    last_ok         :=  boolean(),
    completed       :=  [reliable_task:t()],
    nbr_of_tasks    :=  non_neg_integer(),
    count           :=  non_neg_integer()
}.


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
    WorkerName :: atom(), StoreRef :: atom(), Bucket :: binary()) ->
    {ok, pid()} | {error, any()}.

start_link(WorkerName, StoreRef, Bucket) ->
    gen_server:start({local, WorkerName}, ?MODULE, [StoreRef, Bucket], []).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([StoreRef, Bucket]) ->
    ?LOG_DEBUG("Initializing partition store; partition=~p", [Bucket]),

    Conn = get_db_connection(),
    %% Initialize symbolic variable dict.
    Symbolics = dict:store(riakc, Conn, dict:new()),
    State0 = #state{
        store_ref = StoreRef,
        bucket = Bucket,
        symbolics = Symbolics
    },
    State1 = reset_work_state(State0),
    {ok, State1, {continue, schedule_work}}.


handle_continue(schedule_work, State0) ->
    State1 = schedule_work(State0),
    {noreply, State1}.


handle_call(Msg, From, State) ->
    ?LOG_WARNING(#{
        reason => "Unhandled call",
        message => Msg,
        from => From
    }),
    {reply, {error, not_implemented}, State}.


handle_cast(Msg, State) ->
    ?LOG_WARNING(#{
        reason => "Unhandled cast",
        message => Msg
    }),
    {noreply, State}.


handle_info(
    {timeout, Ref, fetch_work}, #state{fetch_timer_ref = Ref} = State0) ->
    try process_work(State0) of
        {ok, State1} ->
            State = schedule_work(succeed, State1),
            {noreply, State}
    catch
        error:Reason when Reason == overload orelse Reason == timeout ->
            State1 = schedule_work(fail, State0),
            {noreply, State1}
    end;

handle_info(Msg, State) ->
    ?LOG_WARNING(#{
        reason => "Unhandled info",
        message => Msg
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
get_db_connection() ->
    Host = reliable_config:riak_host(),
    Port = reliable_config:riak_port(),

    {ok, Conn} = riakc_pb_socket:start_link(Host, Port),
    pong = riakc_pb_socket:ping(Conn),
    ?LOG_DEBUG(#{
        message => "Got connection to Riak",
        pid => Conn
    }),
    Conn.


%% @private
schedule_work(State) ->
    Floor = reliable_config:get(pull_backoff_min, 2000),
    Ceiling = reliable_config:get(pull_backoff_max, 60000),
    B = backoff:type(
        backoff:init(Floor, Ceiling, self(), fetch_work),
        jitter
    ),
    ?LOG_DEBUG(#{
        pid => self(),
        message => "Fetch work scheduled",
        delay => backoff:get(B)
    }),
    State#state{
        fetch_backoff = B,
        fetch_timer_ref = backoff:fire(B)
    }.


schedule_work(succeed, #state{fetch_backoff = B0} = State) ->
    {_, B1} = backoff:succeed(B0),
    ?LOG_DEBUG(#{
        pid => self(),
        message => "Fetch work scheduled",
        delay => backoff:get(B1)
    }),
    State#state{
        fetch_backoff = B1,
        fetch_timer_ref = backoff:fire(B1)
    };

schedule_work(fail, #state{fetch_backoff = B0} = State) ->
    {_, B1} = backoff:fail(B0),
    ?LOG_DEBUG(#{
        pid => self(),
        message => "Fetch work scheduled",
        delay => backoff:get(B1)
    }),
    State#state{
        fetch_backoff = B1,
        fetch_timer_ref = backoff:fire(B1)
    }.


%% @private
process_work(State) ->

    StoreRef = State#state.store_ref,
    Bucket = State#state.bucket,

    LogCtxt = #{
        pid => self(),
        partition => Bucket
    },

    ?LOG_DEBUG(LogCtxt#{message => "Fetching work"}),

    %% We retrieve the work list from the partition store server

    Opts = #{max_results => 100},
    {WorkList, _Cont} = reliable_partition_store:list(StoreRef, Opts),

    %% Iterate through work that needs to be done.
    %% We do not use the continuation, we simply query again on the next
    %% scheduled run.
    Acc = {[], State},
    {Completed, State1} = lists:foldl(fun process_work/2, Acc, WorkList),

    ?LOG_DEBUG(LogCtxt#{
        message => "Completed work",
        work_ids => Completed
    }),

    {ok, State1}.


%% @private
process_work(Work, {Acc, State0}) ->
    StoreRef = State0#state.store_ref,
    Bucket = State0#state.bucket,
    WorkId = reliable_work:id(Work),
    Payload = reliable_work:event_payload(Work),
    N = reliable_work:nbr_of_tasks(Work),

    LogCtxt = #{
        pid => self(),
        work_id => WorkId,
        partition => Bucket,
        event_payload => Payload,
        nbr_of_tasks => N
    },

    ?LOG_DEBUG(LogCtxt#{message => "Performing work"}),

    %% Only remove the work when all of the work items are done.
    case do_process_work(Work, State0) of
        #state{work_state = #{last_ok := true}} = State1 ->
            %% We made it through the entire list with a result for
            %% everything.
            %% At the moment we are removing but we should update instead and
            %% let the store backend decide where to store the completed
            %% work so that users can check and report.
            ?LOG_DEBUG(LogCtxt#{
                message => "Work completed, attempting delete from store"
            }),

            ok = reliable_partition_store:delete(StoreRef, WorkId),
            WorkRef = reliable_work:ref(StoreRef, Work),
            Payload = reliable_work:event_payload(Work),
            Event = {reliable_event, #{
                status => completed,
                work_ref => WorkRef,
                payload => Payload
            }},
            ok = reliable_event_manager:notify(Event),
            {Acc ++ [WorkId], State1};
        #state{work_state = #{last_ok := false}} = State1 ->
            ?LOG_DEBUG(LogCtxt#{message => "Work NOT YET completed"}),
            {Acc, State1}
    end.


%% @private
reset_work_state(State) ->
    WorkState = #{
        last_ok => true,
        work => undefined,
        completed => [],
        count => 0,
        nbr_of_tasks => 0
    },
    State#state{work_state = WorkState}.


%% @private
do_process_work(Work, State0) ->
    Tasks = reliable_work:tasks(Work),
    WorkState = #{
        last_ok => true,
        work => Work,
        completed => [],
        nbr_of_tasks => length(Tasks),
        count => 0
    },
    State = State0#state{work_state = WorkState},
    lists:foldl(fun process_tasks/2, State, Tasks).


%% @private
process_tasks(Last, #state{work_state = #{last_ok := false} = WS0} = State) ->
    Work = maps:get(work, WS0),
    %% Don't iterate if the last item wasn't completed.
    ?LOG_INFO(#{
        message => "Not attempting next item, since last failed.",
        work_id => reliable_work:id(Work)
    }),
    WS1 = WS0#{
        count => maps:get(count, WS0) + 1,
        completed => maps:get(completed, WS0) ++ [Last]
    },
    State#state{work_state = WS1};


process_tasks(Last, #state{work_state = #{last_ok := true} = WS0} = State) ->
    {TaskId, Task0} = Last,
    Work = maps:get(work, WS0),

    ?LOG_DEBUG(#{
        message => "Found task to be performed.",
        work_id => reliable_work:id(Work),
        task_id => TaskId,
        task => Task0
    }),

    case reliable_task:result(Task0) of
        undefined ->
            {Bool, NewWork} = do_process_task({TaskId, Task0}, State),
            WS1 = WS0#{
                last_ok => Bool,
                count => maps:get(count, WS0) + 1,
                completed => maps:get(completed, WS0) ++ [Last],
                work => NewWork
            },
            State#state{work_state = WS1};
        _ ->
            %% Task already had a result, it was processed before
            WS1 = WS0#{
                last_ok => true,
                count => maps:get(count, WS0) + 1,
                completed => maps:get(completed, WS0) ++ [Last]
            },
            State#state{work_state = WS1}
    end.



do_process_task({TaskId, Task0}, State) ->
    %% Destructure work to be performed.
    #{work := Work} = State#state.work_state,
    StoreRef = State#state.store_ref,

    LogCtxt = #{
        work_id => reliable_work:id(Work),
        task_id => TaskId
    },

    %% Attempt to perform task.
    try
        Node = reliable_task:node(Task0),
        Module = reliable_task:module(Task0),
        Function = reliable_task:function(Task0),
        Args = replace_symbolics(reliable_task:args(Task0), State),

        ?LOG_DEBUG(LogCtxt#{
            message => "Trying to perform task",
            task => Task0,
            node => Node,
            module => Module,
            function => Function,
            args => Args
        }),

        Result = rpc:call(Node, Module, Function, Args),

        Task1 = reliable_task:set_result(Result, Task0),

        ?LOG_DEBUG(LogCtxt#{
            message => "Task result",
            result => Result
        }),

        %% Update task
        NewWork = reliable_work:update_task(TaskId, Task1, Work),

        case maybe_update(StoreRef, NewWork, State) of
            true ->
                ?LOG_DEBUG(LogCtxt#{message => "Work updated in store"}),
                {true, NewWork};
            false ->
                ?LOG_DEBUG(LogCtxt#{
                    message => "Updating work in store delayed"
                }),
                {true, NewWork};
            {error, Reason} ->
                throw(Reason)
        end
    catch
        throw:EReason ->
            ?LOG_DEBUG(LogCtxt#{
                message => "Updating work in store failed",
                reason => EReason
            }),
            {false, Work};
        Class:EReason:Stacktrace ->
            ?LOG_ERROR(LogCtxt#{
                message => "Exception while performing task",
                reason => EReason,
                class => Class,
                stacktrace => Stacktrace
            }),
            {false, Work}
    end.


%% @private
maybe_update(StoreRef, Work, State) ->
    WS = State#state.work_state,
    Count = maps:get(count, WS),
    N = maps:get(nbr_of_tasks, WS),
    Divisor = trunc(math:log2(max(2, N))),

    %% We use log2 as a simple flow control mechanism, to avoid writing to
    %% store for every task update. For example, this will result in:
    %% 1 write when N = 1
    %% 2 writes when N = 5
    %% 3 writes when N = 100
    %% 16 writes when N = 100
    %% 111 writes when N = 1000
    %% Plus the final write when the whole work is finished
    case Count rem Divisor == 0 of
        true ->
            case reliable_partition_store:update(StoreRef, Work) of
                ok -> true;
                Error -> Error
            end;
        false ->
            false
    end.


%% @private
replace_symbolics(Args, State) ->
    lists:map(
        fun(Arg) ->
            case Arg of
                {symbolic, Symbolic} ->
                    case dict:find(Symbolic, State#state.symbolics) of
                        error ->
                            Arg;
                        {ok, Value} ->
                            Value
                    end;
                _ ->
                    Arg
            end
        end,
        Args
    ).