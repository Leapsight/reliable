%% =============================================================================
%%  reliable_partition_store.erl -
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

-module(reliable_partition_store).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("reliable.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-record(state, {
    bucket                  ::  binary(),
    dlq_bucket              ::  binary(),
    backend                 ::  module(),
    backend_ref             ::  reference() | pid(),
    queue                   ::  reliable_queue:t() | undefined
}).


-type list_opts()           ::  optional(#{max_results => pos_integer()}).
-type opts()                ::  optional(any()).

%% API
-export([count/1]).
-export([count/2]).
-export([delete/2]).
-export([delete/3]).
-export([delete_all/2]).
-export([delete_all/3]).
-export([delete_all/4]).
-export([enqueue/2]).
-export([enqueue/3]).
-export([enqueue/4]).
-export([flush/1]).
-export([flush/2]).
-export([flush/3]).
-export([flush_all/0]).
-export([flush_all/1]).
-export([list/1]).
-export([list/2]).
-export([list/3]).
-export([move_to_dlq/3]).
-export([move_to_dlq/4]).
-export([start_link/2]).
-export([status/1]).
-export([status/2]).
-export([status/3]).
-export([update/2]).
-export([update/3]).

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




-spec start_link(Name :: atom(), Bucket :: binary()) ->
    {ok, pid()} | ignore | {error, any()}.

start_link(Name, Bucket) ->
    gen_server:start_link({local, Name}, ?MODULE, [Bucket], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkRef :: reliable_work_ref:t() | binary()) ->
    {in_progress, Info :: reliable_work:status()}
    | {failed, Info :: reliable_work:status()}
    | {error, not_found | timeout | badref | any()}.

status(WorkRef) ->
    status(WorkRef, 5000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkRef :: reliable_work_ref:t() | binary(), Timeout :: timeout()) ->
    {in_progress, Info :: reliable_work:status()}
    | {failed, Info :: reliable_work:status()}
    | {error, not_found | timeout | badref | any()}.

status(Bin, Timeout) when is_binary(Bin) ->
    status(reliable_work_ref:decode(Bin), Timeout);

status(WorkRef, Timeout) ->
    try reliable_work_ref:store_ref(WorkRef) of
        Name ->
            status(Name, WorkRef, Timeout)
    catch
        error:badref ->
            {error, badref}
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(Name :: atom(), WorkRef :: reliable_work_ref:t(), timeout()) ->
    {in_progress, Info :: reliable_work:status()}
    | {failed, Info :: reliable_work:status()}
    | {error, not_found | timeout | badref | any()}.

status(Name, WorkRef, Timeout) when is_atom(Name) ->
    WorkId = reliable_work_ref:work_id(WorkRef),
    safe_call(Name, {status, WorkId}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Name :: atom(), Work :: reliable_work:t()) ->
    {ok, reliable_work_ref:t()} | {error, timeout | any()}.

enqueue(Name, Work) ->
    enqueue(Name, Work, undefined, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Name :: atom(), Work :: reliable_work:t(), Opts :: opts()) ->
    {ok, reliable_work_ref:t()} | {error, timeout | any()}.

enqueue(Name, Work, Opts) ->
    enqueue(Name, Work, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    Name :: atom(),
    Work :: reliable_work:t(),
    Opts :: opts(),
    Timeout :: timeout()) ->
    {ok, reliable_work_ref:t()} | {error, timeout | any()}.

enqueue(Name, Work, Opts, Timeout)
when is_atom(Name) andalso ?IS_TIMEOUT(Timeout) ->
    case reliable_work:is_type(Work) of
        true ->
            safe_call(Name, {enqueue, Name, Work, Opts}, Timeout);
        false ->
            {error, {badarg, Work}}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush_all() -> ok | {error, Reason :: timeout | any()}.

flush_all() ->
    flush_all(undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush_all(opts()) -> ok | {error, Reason :: timeout | any()}.

flush_all(Opts) ->
    Stores = supervisor:which_children(reliable_partition_store_sup),
    _ = [
        begin
            %% eqwalizer:ignore Name
            case flush(Name, Opts) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?LOG_ERROR(#{
                        message =>
                            "Error while flushing reliable partition store",
                        reason => Reason,
                        ref => Name
                    }),
                    ok
            end
        end || {Name, _, _, _} <- Stores
    ],
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(Name :: atom()) ->
    ok | {error, Reason :: timeout | any()}.

flush(Name) ->
    flush(Name, undefined).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(Name :: atom(), Opts :: opts()) ->
    ok | {error, Reason :: timeout | any()}.

flush(Name, Opts) ->
    flush(Name, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(Name :: atom(), Opts :: opts(), timeout()) ->
    ok | {error, Reason :: timeout | any()}.

flush(Name, Opts, Timeout) ->
    safe_call(Name, {flush, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(Name :: atom()) ->
    {ok, {[reliable_work:t()], Continuation :: any()}}
    | {error, Reason :: any()}.

list(Name) ->
    list(Name, undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(Name :: atom(), Opts :: list_opts()) ->
    {ok, {[reliable_work:t()], Continuation :: any()}}
    | {error, Reason :: any()}.

list(Name, Opts) ->
    list(Name, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(Name :: atom(), Opts :: list_opts(), Timeout :: timeout()) ->
    {ok, {[reliable_work:t()], Continuation :: any()}}
    | {error, Reason :: any()}.

list(Name, Opts, Timeout) ->
    safe_call(Name, {list, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(Name :: atom()) ->
    {ok, Count :: integer()} | {error, Reason :: any()}.

count(Name) ->
    count(Name, undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(Name :: atom(), opts()) ->
    {ok, Count :: integer()} | {error, Reason :: any()}.

count(Name, Opts) ->
    count(Name, Opts, ?DEFAULT_TIMEOUT).




%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(Name :: atom(), Opts :: opts(), Timeout :: timeout()) ->
    {ok, Count :: integer()} | {error, Reason :: any()}.

count(Name, Opts, Timeout) ->
    safe_call(Name, {count, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(Name :: atom(), Work :: reliable_work:t()) ->
    ok | {error, Reason :: any()}.

update(Name, Work) ->
    update(Name, Work, undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Name :: atom(), Work :: reliable_work:t(), Opts :: any()) ->
    ok | {error, Reason :: timeout | any()}.

update(Name, Work, Opts) ->
    update(Name, Work, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Name :: atom(),
    Work :: reliable_work:t(),
    Opts :: any(),
    Timeout :: timeout()) -> ok | {error, Reason :: timeout | any()}.

update(Name, Work, Opts, Timeout) ->
    case reliable_work:is_type(Work) of
        true ->
            safe_call(Name, {update, Work, Opts}, Timeout);
        false ->
            error({badarg, Work})
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(Name :: atom(), Term :: reliable_work:id() | reliable_work:t()) ->
    ok | {error, Reason :: timeout | any()}.

delete(Name, Term) ->
    delete(Name, Term, undefined).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(
    Name :: atom(),
    Term :: reliable_work:id() | reliable_work:t(),
    Opts :: opts()) -> ok | {error, Reason :: timeout | any()}.

delete(Name, Term, Opts) ->
    delete(Name, Term, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(
    Name :: atom(),
    Term :: reliable_work:id() | reliable_work:t(),
    Opts :: opts(),
    Timeout :: timeout()) -> ok | {error, Reason :: timeout | any()}.

delete(Name, Term, Opts, Timeout) ->
    WorkId =
        case reliable_work:is_type(Term) of
            true ->
                %% eqwalizer:ignore
                reliable_work:id(Term);
            false ->
                Term
        end,
    %% eqwalizer:ignore
    delete_all(Name, [WorkId], Opts, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec move_to_dlq(Name :: atom(), Work :: reliable_work:t(), Opts :: opts()) ->
    ok | {error, Reason :: any()}.

move_to_dlq(Name, Work, Opts) ->
    move_to_dlq(Name, Work, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec move_to_dlq(
    Name :: atom(),
    Work :: reliable_work:t(),
    Opts :: opts(),
    Timeout :: timeout()) -> ok | {error, Reason :: timeout | any()}.

move_to_dlq(Name, Work, Opts, Timeout) ->
    case reliable_work:is_type(Work) of
        true ->
            safe_call(Name, {move_to_dlq, Name, Work, Opts}, Timeout);
        false ->
            error({badarg, Work})
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(Name :: atom(), WorkIds :: [reliable_work:id()]) ->
    ok | {error, Reason :: timeout | any()}.

delete_all(Name, WorkIds) ->
    delete_all(Name, WorkIds, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Name :: atom(),
    WorkIds :: [reliable_work:id()],
    Timeout :: timeout()) -> ok | {error, Reason :: timeout | any()}.

delete_all(Name, WorkIds, Opts) when is_list(WorkIds)->
    delete_all(Name, WorkIds, Opts, ?DEFAULT_TIMEOUT).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Name :: atom(),
    WorkIds :: [reliable_work:id()],
    Opts :: opts(),
    Timeout :: timeout()) -> ok | {error, Reason :: timeout | any()}.

delete_all(Name, WorkIds, Opts, Timeout) when is_list(WorkIds)->
    safe_call(Name, {delete, WorkIds, Opts}, Timeout).



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
                dlq_bucket = <<Bucket/binary, "_dlq">>,
                backend = BackendMod,
                backend_ref = Ref
            },
            {ok, State, {continue, setup_queue}};

        {error, Reason} ->
            {stop, {error, Reason}}
    end.


handle_continue(setup_queue, State0) ->
    State =
        case reliable_config:get(local_queue, undefined) of
            undefined ->
                State0;

            #{enabled := false} ->
                State0;

            #{enabled := true} = Opts ->
                Q = reliable_queue:new(State0#state.bucket, Opts),
                State0#state{queue = Q}
        end,

    {noreply, State}.


handle_call({enqueue, Name, Work, Opts}, From, #state{} = State) ->
    BackendMod = State#state.backend,
    BackendRef = State#state.backend_ref,
    Bucket = State#state.bucket,
    case do_enqueue(Name, BackendMod, BackendRef, Bucket, Work, Opts) of
        {ok, WorkRef} ->
            _ = gen_server:reply(From, {ok, WorkRef}),
            Payload = reliable_work:event_payload(Work),
            Event = {
                reliable_event,
                #{
                    status => scheduled,
                    work_ref => WorkRef,
                    payload => Payload
                }
            },
            ok = reliable_event_manager:notify(Event),
            {noreply, State};

        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({status, WorkId}, _From, State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,

    Result = case BackendMod:get(Ref, Bucket, WorkId, undefined) of
        {ok, Work} ->
            {in_progress, reliable_work:status(Work)};

        {error, _} = Error ->
            Error
    end,

    {reply, Result, State};

handle_call({count, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:count(Ref, Bucket, Opts),
    {reply, Reply, State};

handle_call({list, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:list(Ref, Bucket, Opts),
    {reply, Reply, State};

handle_call({flush, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:flush(Ref, Bucket, Opts),
    {reply, Reply, State};

handle_call({update, Work, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:update(Ref, Bucket, Work, Opts),
    {reply, Reply, State};

handle_call({delete, WorkIds, Opts}, _From, #state{} = State) ->
    BackendMod = State#state.backend,
    Ref = State#state.backend_ref,
    Bucket = State#state.bucket,
    Reply = BackendMod:delete_all(Ref, Bucket, WorkIds, Opts),
    {reply, Reply, State};

handle_call({move_to_dlq, Name, Work, Opts}, From, #state{} = State) ->
    BackendMod = State#state.backend,
    BackendRef = State#state.backend_ref,
    Bucket = State#state.bucket,

    Reply =
        case BackendMod:delete(Name, Bucket, Work) of
            ok ->
                DLQBucket = State#state.dlq_bucket,
                Result = do_enqueue(
                    Name, BackendMod, BackendRef, DLQBucket, Work, Opts
                ),
                case Result of
                    {ok, WorkRef} ->
                        _ = gen_server:reply(From, {ok, WorkRef}),
                        Payload = reliable_work:event_payload(Work),
                        Event = {
                            reliable_event,
                            #{
                                status => moved_to_dlq,
                                work_ref => WorkRef,
                                payload => Payload
                            }
                        },
                        ok = reliable_event_manager:notify(Event);

                    {error, _} = Error ->
                        Error
                end;

            {error, _} = Error ->
                Error
        end,
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


safe_call(ServerRef, Request, Timeout) when ?IS_TIMEOUT(Timeout) ->
    Ref = gen_server:send_request(ServerRef, Request),

    case gen_server:wait_response(Ref, Timeout) of
        {reply, Response} ->
            Response;
        {error, {Reason, ServerRef}} ->
            {error, Reason};
        timeout ->
            {error, timeout}
    end.


%% @private
do_enqueue(Name, BackendMod, Ref, Bucket, Work, Opts) ->
    WorkId = reliable_work:id(Work),
    case BackendMod:enqueue(Ref, Bucket, Work, Opts) of
        ok ->
            ?LOG_INFO(#{
                message => "Enqueued work",
                work_id => WorkId,
                store_ref => Name,
                instance => Bucket
            }),
            WorkRef = reliable_work:ref(Name, Work),
            {ok, WorkRef};

        {error, _} = Error ->
            ?LOG_INFO(#{
                message => "Enqueuing error",
                work_id => WorkId,
                store_ref => Name,
                instance => Bucket
            }),
           Error
    end.
