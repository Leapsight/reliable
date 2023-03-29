%% =============================================================================
%%  reliable_riak_store_backend.erl -
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

-module(reliable_riak_store_backend).
-behaviour(reliable_store_backend).

-include_lib("kernel/include/logger.hrl").
-include_lib("riakc/include/riakc.hrl").
-include("reliable.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-define(POOLNAME, reliable).

-export([init/0]).
-export([enqueue/3]).
-export([enqueue/4]).
-export([get/3]).
-export([delete/3]).
-export([delete_all/3]).
-export([update/3]).
-export([update/4]).
-export([list/3]).
-export([flush/2]).
-export([fold/5]).
-export([count/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init() ->
    case riak_pool:checkout(?POOLNAME, #{timeout => 2000}) of
        {ok, Pid} ->
            ok = riak_pool:checkin(?POOLNAME, Pid, ok),
            {ok, ?POOLNAME};

        {error, Reason} = Error ->
            ?LOG_INFO(#{
                message => "Error while getting db connection from pool.",
                poolname => reliable,
                reason => Reason
            }),
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t()) -> ok | {error, any()}.

enqueue(Ref, Bucket, Work) ->
    enqueue(Ref, Bucket, Work, #{timeout => 15000}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    PoolOpts :: riak_pool:opts()) -> ok | {error, any()}.

enqueue(Ref, Bucket, Work, Opts) ->
    WorkId = reliable_work:id(Work),

    Fun = fun(Pid) ->
        Object = riakc_obj:new(Bucket, WorkId, term_to_binary(Work)),

        case riakc_pb_socket:put(Pid, Object) of
            ok ->
                ok;

            {error, Reason} ->
                %% Determines if error reason is recoverable or not
                %% Log (Info|Error) if (yes|no)
                %% If server error or timeout and recoverable but not overload,
                %% might retry with backoff (this blocks the worker but the next
                %% workflow will probably fail with this reason too) if allowed
                %% by Opts
                maybe_throw(format_reason(Reason))
        end
    end,

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            Result;

        {error, _} = Error ->
            %% TODO If unrecoverable, send to Quarantine queue, Dead-letter
            %% queue
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) ->
    {ok, reliable_work:t()} | {error, not_found | any()}.

get(Ref, Bucket, WorkId) ->
    Opts = #{timeout => ?DEFAULT_TIMEOUT},

    Fun = fun(Pid) ->
        get_work(Pid, Bucket, WorkId, [])
    end,

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            Result;
        {error, _} = Error ->
            %% If busy do retries
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(
    Ref :: reliable_store_backend:ref() | atom(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) -> ok | {error, Reason :: any()}.

delete(Pid, Bucket, WorkId) when is_pid(Pid) ->
    case riakc_pb_socket:delete(Pid, Bucket, WorkId) of
        ok ->
            ok;
        {error, Reason} ->
            {error, format_reason(Reason)}
    end;

delete(Ref, Bucket, WorkId) when is_atom(Ref) ->
    Fun = fun(Pid) ->
        case delete(Pid, Bucket, WorkId) of
            ok ->
                ok;
            {error, Reason} ->
                maybe_throw(Reason)
        end
    end,

    Opts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            Result;
        {error, _} = Error ->
            %% If busy do retries
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()]) -> ok | {error, Reason :: any()}.

delete_all(Ref, Bucket, WorkIds) ->
    Opts = #{timeout => ?DEFAULT_TIMEOUT},

    Fun = fun(Pid) ->
        _ = lists:foreach(
            fun(WorkId) ->
                case delete(Pid, Bucket, WorkId) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        %% Force riak_pool to retry, depending on default opts
                        maybe_throw(Reason)
                end
            end,
            WorkIds
        ),
        ok
    end,

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            Result;

        {error, _} = Error ->
            %% If busy do retries
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t()) -> ok | {error, any()}.

update(Ref, Bucket, Work) ->
    update(Ref, Bucket, Work, #{timeout => ?DEFAULT_TIMEOUT}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: riak_pool:exec_opts()) -> ok | {error, any()}.

update(Ref, Bucket, Work, Opts) ->
    WorkId = reliable_work:id(Work),

    Fun = fun(Pid) ->
        case riakc_pb_socket:get(Pid, Bucket, WorkId) of
            {ok, Obj} ->
                do_update(Pid, Bucket, Work, Obj);

            {error, Reason} ->
                ?LOG_ERROR(#{
                    message => "failed to read object before update.",
                    work_id => WorkId,
                    bucket => Bucket,
                    reason => Reason
                }),
                {error, format_reason(Reason)}
        end
    end,

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            Result;

        {error, _} = Error ->
            %% If busy do retries
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) -> Count :: integer() | {error, Reason :: any()}.

count(Ref, Bucket, Opts) ->
    PoolOpts = #{timeout => ?DEFAULT_TIMEOUT},

    Fun = fun(Pid) ->
        Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
        Inputs = {index, Bucket, <<"$bucket">>, <<>>},
        Query = [
            {
                reduce,
                {modfun, riak_kv_mapreduce, reduce_count_inputs},
                [{reduce_phase_batch_size, 1000}] ,
                true
            }
        ],

        case riakc_pb_socket:mapred(Pid, Inputs, Query, Timeout) of
            {ok, [{0, [Count]}]} ->
                Count;

            {error, Reason} ->
                {error, format_reason(Reason)}
        end
    end,

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {ok, Result} ->
            Result;
        {error, _} = Error ->
            %% If busy do retries
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) ->
    List :: {ok, [reliable_work:t()], Continuation :: continuation()}
    | {error, Reason :: any()}.

list(Ref, Bucket, Opts) ->
    Fun = fun({_K, V}, Acc) -> [V | Acc] end,

    case fold(Ref, Bucket, Fun, [], Opts) of
        {ok, {L, Cont}} ->
            {ok, {lists:reverse(L), Cont}};
        {error, _} = Error ->
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(Ref :: reliable_store_backend:ref(), Bucket :: binary()) ->
    ok | {error, Reason :: any()}.

flush(Ref, Bucket) ->
    flush(Ref, Bucket, #{}).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec fold(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Fun :: function(),
    Acc :: any(),
    Opts :: map()) ->
    {ok, {NewAcc :: any(), Continuation :: continuation()}}
    | {error, Reason :: any()}.

fold(Ref, Bucket, Function, Acc, Opts) ->
    Fun = fun(Pid) ->
        ReqOpts = fold_opts(Opts),
        %% Get list of the keys in the bucket.
        %% We use the $bucket secondary index so that we can do pagination with
        %% sorting.
        Res = riakc_pb_socket:get_index_eq(
            Pid,
            Bucket,
            <<"$bucket">>,
            <<>>,
            ReqOpts
        ),

        case Res of
            {ok, #index_results_v1{keys = Keys, continuation = Cont1}} ->
                ?LOG_DEBUG("Got work keys: ~p", [Keys]),

                FoldFun = fun(Key, Acc1) ->
                    %% notfound_ok = true is the default value.
                    %% This parameter determines how Riak responds if a read
                    %% fails on a node.
                    %% Setting to true (the default) is the equivalent to
                    %% setting R to 1: if the first node to respond doesn’t
                    %% have a copy of the object, Riak will immediately return
                    %% a not found error.
                    %% If set to false, Riak will continue to look for the
                    %% object on the number of nodes specified by N (aka n_val
                    %% with default 3).
                    RiakOpts = [{notfound_ok, false}],

                    case get_work(Pid, Bucket, Key, RiakOpts) of
                        {ok, Work} ->
                            ?LOG_DEBUG("got key: ~p, work: ~p", [Key, Work]),
                            Function({Key, Work}, Acc1);

                        {error, Reason} ->
                            case reliable_cache:has(Key) of
                                true ->
                                    ?LOG_DEBUG(#{
                                        description =>
                                            "Couldn't retrieve work from store "
                                            "(not an error, case of a stale "
                                            "$bucket index, item in cache).",
                                        backend => ?MODULE,
                                        reason => Reason,
                                        partition => Bucket,
                                        key => Key
                                    });
                                false ->

                                    ?LOG_WARNING(#{
                                        description =>
                                            "Couldn't retrieve work "
                                            "from store.",
                                        backend => ?MODULE,
                                        reason => Reason,
                                        partition => Bucket,
                                        key => Key
                                    })
                            end,

                            Acc1
                    end
                end,
                {ok, {lists:foldl(FoldFun, Acc, Keys), Cont1}};
            {error, Reason} ->
                {error, format_reason(Reason)}
        end

    end,

    PoolOpts = #{timeout => ?DEFAULT_TIMEOUT},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {ok, {ok, _} = OK} ->
            OK;
        {ok, {error, _} = Error} ->
            Error;
        {error, Reason} = Error ->
            ?LOG_INFO(
                "Could not retrieve work from store; reason=~p",
                [Reason]
            ),
            Error
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
fold_opts(Opts0) ->
    Default = #{
        max_results => 10,
        pagination_sort => true,
        timeout => 30000
    },

    Opts1 = maps:merge(Default, Opts0),

    case maps:get(continuation, Opts1, undefined) of
        undefined ->
            maps:to_list(Opts1);
        Cont0 ->
            maps:to_list(Opts1#{continuation => Cont0})
    end.


%% @private
maybe_throw(Reason)
when Reason == disconnected; Reason == overload; Reason == timeout ->
    %% Throw so that riak_pool can retry according to PoolOpts
    throw(Reason);

maybe_throw(Reason) ->
    {error, Reason}.


%% @private
format_reason(<<"Operation type is", _/binary>>) -> datatype_mismatch;
format_reason(<<"overload">>) -> overload;
format_reason(notfound) -> not_found;
format_reason(Reason) -> Reason.


%% @private
get_work(Ref, Bucket, WorkId, RiakOpts) ->
    case riakc_pb_socket:get(Ref, Bucket, WorkId, RiakOpts) of
        {ok, Object} ->
            object_to_work(Ref, Bucket, WorkId, Object);
        {error, Reason} ->
            {error, format_reason(Reason)}
    end.


%% @private
object_to_work(Ref, Bucket, WorkId, Object) ->
    Data = riakc_obj:get_value(Object),
    Term = binary_to_term(Data),

    case reliable_work:is_type(Term) of
        true ->
            {ok, Term};
        false ->
            ?LOG_ERROR(#{
                message =>
                    "Found invalid work term in store. "
                    "Term will be deleted.",
                bucket => bucket,
                work_id => WorkId,
                term => Term
            }),
            ok = delete(Ref, Bucket, WorkId),
            {error, invalid_term}
    end.


%% @private
do_update(Pid, Bucket, Work, Obj0) when is_pid(Pid) ->
    Obj1 = riakc_obj:update_value(Obj0, term_to_binary(Work)),

    case riakc_pb_socket:put(Pid, Obj1, [return_body]) of
        {ok, _Obj2} ->
            ok;
        {error, Reason} ->
            ?LOG_ERROR(#{
                message => "Failed to update object.",
                work_id => reliable_work:id(Work),
                bucket => Bucket,
                reason => Reason
            }),
            {error, format_reason(Reason)}
    end.


%% @private
-spec flush(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) -> ok | {error, any()}.

flush(Ref, Bucket, Opts) ->
    Fun = fun({K, _}, Acc) ->
        case delete(Ref, Bucket, K) of
            ok ->
                Acc;
            {error, not_found} ->
                Acc;
            {error, Reason} ->
                throw(Reason)
        end
    end,

    try fold(Ref, Bucket, Fun, ok, Opts) of
        {ok, {ok, undefined}} ->
            ok;
        {ok, {ok, Cont}} ->
            flush(Ref, Bucket, Opts#{continuation => Cont})
    catch
        throw:Reason ->
            {error, Reason}
    end.


