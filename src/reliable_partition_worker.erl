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
    store_name              ::  atom(),
    bucket                  ::  binary(),
    symbolics               ::  dict:dict(),
    fetch_backoff           ::  backoff:backoff(),
    fetch_timer_ref         ::  reference() | undefined
}).

%% should be some sort of unique term identifier.
-type work_id() :: binary().

%% MFA, and a node to actually execute the RPC at.
-type work_item() :: {node(), module(), function(), [term()]}.

%% the result of any work performed.
-type work_item_result() :: term().

%% identifier for the work item.
-type work_item_id() :: integer().


-type work_ref()    ::  {
    work_ref,
    Instance :: binary(),
    WorkId :: work_id()
}.

-export_type([work_ref/0]).
-export_type([work_id/0]).
-export_type([work_item_id/0]).
-export_type([work_item_result/0]).
-export_type([work_item/0]).


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
    WorkerName :: atom(), StoreName :: atom(), Bucket :: binary()) ->
    {ok, pid()} | {error, any()}.

start_link(WorkerName, StoreName, Bucket) ->
    gen_server:start({local, WorkerName}, ?MODULE, [StoreName, Bucket], []).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([StoreName, Bucket]) ->
    ?LOG_DEBUG("Initializing partition store; partition=~p", [Bucket]),

    Conn = get_db_connection(),
    %% Initialize symbolic variable dict.
    Symbolics = dict:store(riakc, Conn, dict:new()),

    State = #state{
        store_name = StoreName,
        bucket = Bucket,
        symbolics = Symbolics
    },
    {ok, State, {continue, schedule_work}}.


handle_continue(schedule_work, State0) ->
    State1 = schedule_work(State0),
    {noreply, State1}.


handle_call(Msg, _From, State) ->
    ?LOG_WARNING("Unhandled call; message=~p", [Msg]),
    {reply, {error, not_implemented}, State}.


handle_cast(Msg, State) ->
    ?LOG_WARNING("Unhandled cast; message=~p", [Msg]),
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
   ?LOG_WARNING("Unhandled info; message=~p", [Msg]),
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
    ?LOG_DEBUG("Got connection to Riak: ~p", [Conn]),
    Conn.


%% @private
schedule_work(State) ->
    Floor = 1000,
    Ceiling = 60000,
    B = backoff:type(
        backoff:init(Floor, Ceiling, self(), fetch_work),
        jitter
    ),
    State#state{
        fetch_backoff = B,
        fetch_timer_ref = backoff:fire(B)
    }.


schedule_work(succeed, #state{fetch_backoff = B0} = State) ->
    {_, B1} = backoff:succeed(B0),
    State#state{
        fetch_backoff = B1,
        fetch_timer_ref = backoff:fire(B1)
    };

schedule_work(fail, #state{fetch_backoff = B0} = State) ->
    {_, B1} = backoff:fail(B0),
    State#state{
        fetch_backoff = B1,
        fetch_timer_ref = backoff:fire(B1)
    }.


%% @private
process_work(State) ->
    ?LOG_INFO("Fetching work."),
    StoreName = State#state.store_name,
    Bucket = State#state.bucket,
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
    {WorkList, _Cont} = reliable_partition_store:list(StoreName, Opts),
    {Completed, State} = lists:foldl(fun process_work/2, {[], State}, WorkList),

    ?LOG_DEBUG("Attempting to delete completed work: ~p", [Completed]),

    %% Delete items outside of iterator to ensure delete is safe.
    _ = lists:foreach(
        fun(WorkId) ->
            ok = reliable_partition_store:delete(StoreName, WorkId),
            %% Notify subscribers
            WorkRef = {work_ref, Bucket, WorkId},
            ok = reliable_event_manager:notify({reliable, completed, WorkRef})
        end,
        Completed
    ),

    ok.


%% @private
process_work({WorkId, Items}, {Acc, State}) ->
    ?LOG_DEBUG("Found work to be performed: ~p", [WorkId]),

    %% Only remove the work when all of the work items are done.
    ItemAcc = {true, [], WorkId, Items, State},
    case lists:foldl(fun process_work_items/2, ItemAcc, Items) of
        {true, _, _, _, _} ->
            %% We made it through the entire list with a result for
            %% everything, remove.
            ?LOG_DEBUG("work ~p completed!", [WorkId]),
            {Acc ++ [WorkId], State};
        {false, _, _, _, _} ->
            ?LOG_DEBUG("work ~p NOT YET completed!", [WorkId]),
            {Acc, State}
    end.


%% @private
process_work_items(LastItem, {false, Completed, WorkId, Items, State}) ->
    %% Don't iterate if the last item wasn't completed.
    ?LOG_INFO("Not attempting next item, since last failed."),
    {false, Completed ++ [LastItem], WorkId, Items, State};

process_work_items(
    {ItemId, Item, undefined} = LastItem,
    {true, Completed, WorkId, Items, State}) ->
    ?LOG_INFO("Found work item to be performed: ~p", [Item]),
    %% Destructure work to be performed.
    Symbolics = State#state.symbolics,
    StoreName = State#state.store_name,

    {Node, Module, Function, Args0} = Item,

    %% Attempt to perform work.
    try
        %% Replace symbolic terms in the work item.
        Args = lists:map(fun(Arg) ->
            case Arg of
                {symbolic, Symbolic} ->
                    case dict:find(Symbolic, Symbolics) of
                        error ->
                            Arg;
                        {ok, Value} ->
                            Value
                    end;
                _ ->
                    Arg
            end
        end, Args0),

        ?LOG_DEBUG(
            "Trying to perform work; node=~p, module=~p, "
            "function=~p, args=~p",
            [Node, Module, Function, Args]
        ),

        Result = rpc:call(Node, Module, Function, Args),

        ?LOG_DEBUG("Got result: ~p", [Result]),

        %% Update item.
        NewItems = lists:keyreplace(ItemId, 1, Items, {ItemId, Item, Result}),
        Result = reliable_partition_store:update(StoreName, {WorkId, NewItems}),

        case Result of
            ok ->
                ?LOG_DEBUG("Updated item."),
                {true, Completed ++ [LastItem], WorkId, Items, State};
            {error, Reason} ->
                ?LOG_DEBUG("Writing failed: ~p", [Reason]),
                {false, Completed ++ [LastItem], WorkId, Items, State}
        end
    catch
        _:Error ->
            ?LOG_ERROR("Got exception: ~p", [Error]),
            {false, Completed ++ [LastItem], WorkId, Items, State}
    end;

process_work_items(
    {_, Item, _} = LastItem, {true, Completed, WorkId, Items, State}) ->
    ?LOG_INFO("Found work item to be performed: ~p", [Item]),
    ?LOG_DEBUG("Work already performed, advancing to next item."),
    {true, Completed ++ [LastItem], WorkId, Items, State}.
