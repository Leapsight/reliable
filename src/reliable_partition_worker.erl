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
    fetch_timer_ref         ::  reference() | undefined
}).


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

    State = #state{
        store_ref = StoreRef,
        bucket = Bucket,
        symbolics = Symbolics
    },
    {ok, State, {continue, schedule_work}}.


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
    {timeout, Ref, fetch_work}, #state{fetch_timer_ref = Ref} = State) ->
    try process_work(State) of
        ok ->
            State1 = schedule_work(succeed, State),
            {noreply, State1}
    catch
        error:Reason when Reason == overload orelse Reason == timeout ->
            State1 = schedule_work(fail, State),
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

    ?LOG_DEBUG(#{
        message => "Fetching work.",
        pid => self(),
        partition => Bucket
    }),

    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert
    %% case)
    %% is not as bad as skipping an item (delete case.)

    %% We do not use the continuation, we simply query again on the next
    %% scheduled run.

    %% We retrieve the work list from the partition store server
    Opts = #{max_results => 100},
    {WorkList, _Cont} = reliable_partition_store:list(StoreRef, Opts),
    {Completed, State} = lists:foldl(fun process_work/2, {[], State}, WorkList),

    ?LOG_DEBUG(#{
        pid => self(),
        message => "Completed work",
        work_ids => Completed
    }),

    ok.


%% @private
process_work(Work, {Acc, State}) ->
    WorkId = reliable_work:id(Work),
    Tasks = reliable_work:tasks(Work),
    ?LOG_DEBUG(#{
        pid => self(),
        message => "Found work to be performed",
        work_id => WorkId
    }),

    %% Only remove the work when all of the work items are done.
    ItemAcc = {true, [], Work, State},

    case lists:foldl(fun process_tasks/2, ItemAcc, Tasks) of
        {true, _, _, _} ->
            %% We made it through the entire list with a result for
            %% everything, remove.
            StoreRef = State#state.store_ref,
            Bucket = State#state.bucket,

            ?LOG_DEBUG(#{
                pid => self(),
                message => "Work completed, attempting delete from store",
                work_id => WorkId,
                partition => Bucket
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
            {Acc ++ [WorkId], State};
        {false, _, _, _} ->
            ?LOG_DEBUG(#{
                message => "Work NOT YET completed",
                work_id => WorkId
            }),
            {Acc, State}
    end.


%% @private
process_tasks(Last, {false, Completed, Work, State}) ->
    %% Don't iterate if the last item wasn't completed.
    ?LOG_INFO(#{
        message => "Not attempting next item, since last failed.",
        work_id => reliable_work:id(Work)
    }),
    {false, Completed ++ [Last], Work, State};

process_tasks(
    {TaskId, Task0} = Last, {true, Completed, Work, State}) ->
    ?LOG_DEBUG(#{
        message => "Found task to be performed.",
        work_id => reliable_work:id(Work),
        task_id => TaskId,
        task => Task0
    }),

    case reliable_task:result(Task0) of
        undefined ->
            {Bool, NewWork} = do_process_task({TaskId, Task0}, Work, State),
            {Bool, Completed ++ [Last], NewWork, State};
        _ ->
            {true, Completed ++ [Last], Work, State}
    end.


do_process_task({TaskId, Task0}, Work, State) ->
    %% Destructure work to be performed.
    StoreRef = State#state.store_ref,
    WorkId = reliable_work:id(Work),

    %% Attempt to perform task.
    try
        Node = reliable_task:node(Task0),
        Module = reliable_task:module(Task0),
        Function = reliable_task:function(Task0),
        Args = replace_symbolics(reliable_task:args(Task0), State),

        ?LOG_DEBUG(#{
            message => "Trying to perform task",
            task => Task0,
            node => Node,
            module => Module,
            function => Function,
            args => Args
        }),

        Result = rpc:call(Node, Module, Function, Args),
        Task1 = reliable_task:set_result(Result, Task0),

        ?LOG_DEBUG(#{
            message => "Task result",
            work_id => WorkId,
            task_id => TaskId,
            result => Result
        }),

        %% Update task
        NewWork = reliable_work:update_task(TaskId, Task1, Work),

        case reliable_partition_store:update(StoreRef, NewWork) of
            ok ->
                ?LOG_DEBUG(#{
                    message => "Updated work",
                    work_id => WorkId
                }),
                {true, NewWork};
            {error, Reason} ->
                ?LOG_DEBUG(#{
                    message => "Writing failed",
                    work_id => WorkId,
                    reason => Reason
                }),
                {false, Work}
        end
    catch
        _:EReason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Got exception",
                reason => EReason,
                stacktrace => Stacktrace
            }),
            {false, Work}
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