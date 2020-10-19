%% =============================================================================
%%  reliable_partition_store.erl -
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

-module(reliable_partition_store).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-record(state, {
    bucket                  ::  binary(),
    backend                 ::  module(),
    backend_ref             ::  reference() | pid()
}).

%% should be some sort of unique term identifier.
-type work_id() :: binary().

%% MFA, and a node to actually execute the RPC at.
-type work_item() :: {node(), module(), function(), [term()]}.

%% the result of any work performed.
-type work_item_result() :: term().

%% identifier for the work item.
-type work_item_id() :: integer().

%% the work.
-type work() :: {
    WorkId :: work_id(),
    Payload :: [{work_item_id(), work_item(), work_item_result()}]
}.

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
-export([start_link/2]).
-export([enqueue/2]).
-export([enqueue/3]).
-export([list/2]).
-export([list/3]).
-export([update/2]).
-export([update/3]).
-export([delete/2]).
-export([delete/3]).
-export([delete_all/2]).
-export([delete_all/3]).
-export([status/1]).
-export([status/2]).

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% =============================================================================
%% API
%% =============================================================================



start_link(Name, Bucket) ->
    gen_server:start({local, Name}, ?MODULE, [Bucket], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkerRef :: work_ref()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status(WorkRef) ->
    status(WorkRef, 5000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkerRef :: work_ref(), timeout()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status({work_ref, InstanceName, WorkId}, Timeout) ->
    Instance = binary_to_atom(InstanceName, utf8),
    gen_server:call(Instance, {status, WorkId}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Name :: atom(), Work :: work()) ->
    {ok, work_ref()} | {error, term()}.

enqueue(Name, Work) ->
    enqueue(Name, Work, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Name :: atom(), Work :: work(), Timeout :: timeout()) ->
    {ok, work_ref()} | {error, term()}.

enqueue(Name, Work, Timeout) ->
    gen_server:call(Name, {enqueue, Work}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(Name :: atom(), Opts :: map()) -> {[work()], Continuation :: any()}.

list(Name, Opts) ->
    list(Name, Opts, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(Name :: atom(), Opts :: map(), Timeout :: timeout()) -> [work()].

list(Name, Opts, Timeout) ->
    gen_server:call(Name, {list, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(Name :: atom(), Work :: work()) -> ok | {error, any()}.

update(Name, Work) ->
    update(Name, Work, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(Name :: atom(), Work :: work(), Timeout :: timeout()) ->
    ok | {error, any()}.

update(Name, Work, Timeout) ->
    gen_server:call(Name, {update, Work}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(Name :: atom(), WorkId :: work_id()) ->
    ok | {error, any()}.

delete(Name, WorkId) ->
    delete(Name, WorkId).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(Name :: atom(), WorkIdOrList :: work_id(), Timeout :: timeout()) ->
    ok | {error, any()}.

delete(Name, WorkId, Timeout) ->
    delete_all(Name, [WorkId], Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(Name :: atom(), WorkIds :: [work_id()]) ->
    ok | {error, any()}.

delete_all(Name, WorkIds) ->
    delete_all(Name, WorkIds, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Name :: atom(), WorkIds :: [work_id()], Timeout :: timeout()) ->
    ok | {error, any()}.

delete_all(Name, WorkIds, Timeout) when is_list(WorkIds)->
    gen_server:call(Name, {delete, WorkIds}, Timeout).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([Bucket]) ->
    ?LOG_DEBUG("Initializing."),

    BackendMod = reliable_config:storage_backend(),

    case BackendMod:init() of
        {ok, Ref} ->
            State = #state{
                bucket = Bucket,
                backend = BackendMod,
                backend_ref = Ref
            },
            {ok, State};

        {error, Reason} ->
            {stop, {error, Reason}}
    end.


handle_call({enqueue, Work}, From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    case do_enqueue(BackendMod, Ref, Bucket, Work) of
        {ok, WorkRef} ->
            _ = gen_server:reply(From, {ok, WorkRef}),
            ok = reliable_event_manager:notify({reliable, scheduled, WorkRef}),
            {noreply, State};
        {error, _} = Error ->
            {reply, Error, State}
    end;


handle_call({status, WorkId}, _From, State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,

    Result = case BackendMod:get(Ref, Bucket, WorkId) of
        {ok, WorkItems} ->
            Remaining = lists:sum([1 || {_, _, undefined} <- WorkItems]),
            Info = #{
                work_id => WorkId,
                instance => Bucket,
                items => length(WorkItems),
                remaining_items => Remaining
            },
            {in_progress, Info};
        {error, _} = Error ->
            Error
    end,

    {reply, Result, State};

handle_call({list, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:list(Ref, Bucket, Opts),
    {reply, Reply, State};

handle_call({update, {WorkId, WorkItems}}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:update(Ref, Bucket, WorkId, WorkItems),
    {reply, Reply, State};

handle_call({delete, WorkIds}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:delete_all(Ref, Bucket, WorkIds),
    {reply, Reply, State};

handle_call(Msg, _From, State) ->
    ?LOG_WARNING("Unhandled call; message=~p", [Msg]),
    {reply, {error, not_implemented}, State}.


handle_cast(Msg, State) ->
    ?LOG_WARNING("Unhandled cast; message=~p", [Msg]),
    {noreply, State}.


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
do_enqueue(BackendMod, Ref, Bucket, {WorkId, _} = Work) ->
    %% TODO: Deduplicate here.
    %% TODO: Replay once completed.
    ?LOG_INFO("Enqueuing work: ~p, instance: ~p", [Work, Bucket]),

    case BackendMod:enqueue(Ref, Bucket, Work) of
        ok ->
            {ok, {work_ref, Bucket, WorkId}};
        {error, _} = Error ->
           Error
    end.
