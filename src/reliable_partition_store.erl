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
-export([status/3]).

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




-spec start_link(Name :: atom(), Bucket :: binary()) ->
    {ok, pid()} | {error, any()}.

start_link(Name, Bucket) ->
    gen_server:start({local, Name}, ?MODULE, [Bucket], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkRef :: reliable_work:ref()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status(WorkRef) ->
    status(WorkRef, 5000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkRef :: reliable_work:ref(), timeout()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status(WorkRef, Timeout) ->
    Instance = reliable_work_ref:instance(WorkRef),
    StoreRef = binary_to_atom(Instance, utf8),
    status(StoreRef, WorkRef, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(StoreRef :: atom(), WorkRef :: reliable_work:ref(), timeout()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status(StoreRef, WorkRef, Timeout) when is_atom(StoreRef) ->
    WorkId = reliable_work_ref:id(WorkRef),
    gen_server:call(StoreRef, {status, WorkId}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(StoreRef :: atom(), Work :: reliable_work:t()) ->
    {ok, reliable_work:ref()} | {error, term()}.

enqueue(StoreRef, Work) ->
    enqueue(StoreRef, Work, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    StoreRef :: atom(), Work :: reliable_work:t(), Timeout :: timeout()) ->
    {ok, reliable_work:ref()} | {error, term()}.

enqueue(StoreRef, Work, Timeout) when is_atom(StoreRef) ->
    case reliable_work:is_type(Work) of
        true ->
            gen_server:call(StoreRef, {enqueue, Work}, Timeout);
        false ->
            error({badarg, Work})
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(StoreRef :: atom(), Opts :: map()) ->
    {[reliable_work:t()], Continuation :: any()}.

list(StoreRef, Opts) ->
    list(StoreRef, Opts, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(StoreRef :: atom(), Opts :: map(), Timeout :: timeout()) ->
    {[reliable_work:t()], Continuation :: any()}.

list(StoreRef, Opts, Timeout) ->
    gen_server:call(StoreRef, {list, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(StoreRef :: atom(), Work :: reliable_work:t()) ->
    ok | {error, any()}.

update(StoreRef, Work) ->
    update(StoreRef, Work, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    StoreRef :: atom(), Work :: reliable_work:t(), Timeout :: timeout()) ->
    ok | {error, any()}.

update(StoreRef, Work, Timeout) ->
    case reliable_work:is_type(Work) of
        true ->
            gen_server:call(StoreRef, {update, Work}, Timeout);
        false ->
            error({badarg, Work})
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(StoreRef :: atom(), WorkId :: reliable_work:id()) ->
    ok | {error, any()}.

delete(StoreRef, WorkId) ->
    delete(StoreRef, WorkId, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(
    StoreRef :: atom(), WorkId :: reliable_work:id(), Timeout :: timeout()) ->
    ok | {error, any()}.

delete(StoreRef, WorkId, Timeout) ->
    delete_all(StoreRef, [WorkId], Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(StoreRef :: atom(), WorkIds :: [reliable_work:id()]) ->
    ok | {error, any()}.

delete_all(StoreRef, WorkIds) ->
    delete_all(StoreRef, WorkIds, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    StoreRef :: atom(),
    WorkIds :: [reliable_work:id()],
    Timeout :: timeout()) -> ok | {error, any()}.

delete_all(StoreRef, WorkIds, Timeout) when is_list(WorkIds)->
    gen_server:call(StoreRef, {delete, WorkIds}, Timeout).



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

handle_call({update, Work}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    WorkId = reliable_work:id(Work),
    Reply = BackendMod:update(Ref, Bucket, WorkId, Work),
    {reply, Reply, State};

handle_call({delete, WorkIds}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:delete_all(Ref, Bucket, WorkIds),
    {reply, Reply, State};

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
do_enqueue(BackendMod, Ref, Bucket, Work) ->
    %% TODO: Deduplicate here.
    %% TODO: Replay once completed.
    WorkId = reliable_work:id(Work),
    ?LOG_INFO(#{
        message => "Enqueuing work",
        work_id => WorkId,
        instance => Bucket
    }),

    case BackendMod:enqueue(Ref, Bucket, Work) of
        ok ->
            WorkRef = reliable_work:ref(Bucket, Work),
            {ok, WorkRef};
        {error, _} = Error ->
           Error
    end.
