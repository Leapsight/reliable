%% =============================================================================
%%  reliable_riak_store_backend.erl -
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

-module(reliable_riak_store_backend).
-behaviour(reliable_store_backend).

-include_lib("kernel/include/logger.hrl").
-include_lib("riakc/include/riakc.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-define(POOLNAME, reliable).

-export([init/0]).
-export([enqueue/3]).
-export([get/3]).
-export([delete/3]).
-export([delete_all/3]).
-export([update/3]).
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
    WorkId = reliable_work:id(Work),

    Fun = fun(Pid) ->
        Object = riakc_obj:new(Bucket, WorkId, term_to_binary(Work)),
        riakc_pb_socket:put(Pid, Object)
    end,

    PoolOpts = #{timeout => 10000},

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
-spec get(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) ->
    {ok, reliable_work:t()} | {error, not_found | any()}.

get(Ref, Bucket, WorkId) ->
    Fun = fun(Pid) -> get_work(Pid, Bucket, WorkId, []) end,
    PoolOpts = #{timeout => 10000},

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
-spec delete(
    Ref :: reliable_store_backend:ref() | atom(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) -> ok | {error, Reason :: any()}.

delete(Pid, Bucket, WorkId) when is_pid(Pid) ->
    case riakc_pb_socket:delete(Pid, Bucket, WorkId) of
        ok -> ok;
        {error, Reason} -> {error, format_reason(Reason)}
    end;

delete(Ref, Bucket, WorkId) when is_atom(Ref) ->
    Fun = fun(Pid) -> delete(Pid, Bucket, WorkId) end,

    PoolOpts = #{timeout => 10000},

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
-spec delete_all(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()]) -> ok | {error, Reason :: any()}.

delete_all(Ref, Bucket, WorkIds) ->
    Fun = fun(Pid) ->
        _ = lists:foreach(
            fun(WorkId) -> delete(Pid, Bucket, WorkId) end,
            WorkIds
        ),
        ok
    end,

    PoolOpts = #{timeout => 10000},

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
-spec update(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t()) -> ok | {error, any()}.

update(Ref, Bucket, Work) ->
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

    PoolOpts = #{timeout => 10000},

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
-spec count(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) -> Count :: integer() | {error, Reason :: any()}.

count(Ref, Bucket, Opts) ->
    Fun = fun(Pid) ->
        Timeout = maps:get(timeout, Opts, 30000),
        Inputs = {index, Bucket, <<"$bucket">>, <<>>},
        Query = [
            {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, [{reduce_phase_batch_size, 1000}] , true}
        ],

        case riakc_pb_socket:mapred(Pid, Inputs, Query, Timeout) of
            {ok, [{0, [Count]}]} ->
                Count;
            {error, Reason} ->
                {error, format_reason(Reason)}
        end
    end,

    PoolOpts = #{timeout => 10000},
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
    List :: {[reliable_work:t()], Continuation :: continuation()}
    | {error, Reason :: any()}.

list(Ref, Bucket, Opts) ->
    Fun = fun({_K, V}, Acc) -> [V | Acc] end,
    {L, Cont} = fold(Ref, Bucket, Fun, [], Opts),
    {lists:reverse(L), Cont}.


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
    {NewAcc :: any(), Continuation :: continuation()}
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
                    case get_work(Pid, Bucket, Key, []) of
                        {ok, Work} ->
                            ?LOG_DEBUG("got key: ~p", [Key]),
                            ?LOG_DEBUG("got work: ~p", [Work]),
                            Function({Key, Work}, Acc1);
                        {error, Reason} ->
                            ?LOG_WARNING(
                                "Could not retrieve work from store (it can well be a stale Riak KV Secondary index); "
                                "backend=~p, reason=~p, partition=~p, key=~p",
                                [?MODULE, Reason, Bucket, Key]
                            ),
                            Acc1
                    end
                end,
                Result = lists:foldl(FoldFun, Acc, Keys),
                {Result, Cont1};
            {error, Reason} ->
                {error, format_reason(Reason)}
        end

    end,

    PoolOpts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {ok, Result} ->
            Result;
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
format_reason("overload") -> overload;
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
        {ok, undefined} ->
            ok;
        {ok, Cont} ->
            flush(Ref, Bucket, Opts#{continuation => Cont})
    catch
        throw:Reason ->
            {error, Reason}
    end.

