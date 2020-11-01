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
-export([get/2]).
-export([delete/3]).
-export([delete_all/3]).
-export([update/4]).
-export([list/3]).
-export([fold/5]).



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
    Tasks =  reliable_work:tasks(Work),

    Fun = fun(Pid) ->
        Object = riakc_obj:new(Bucket, WorkId, term_to_binary(Tasks)),
        riakc_pb_socket:put(Pid, Object)
    end,

    PoolOpts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {true, ok} ->
            ok;
        {false, Reason} ->
            %% If busy do retries
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(
    Ref :: reliable_store_backend:ref(), WorkRef :: reliable_work_ref:t()) ->
    {ok, reliable_work:t()} | {error, not_found | any()}.

get(Ref, WorkRef) ->
    case reliable_work_ref:is_type(WorkRef) of
        true ->
            WorkId = reliable_work_ref:work_id(WorkRef),
            Bucket = reliable_work_ref:instance(WorkRef),

            Fun = fun(Pid) -> get_work(Pid, Bucket, WorkId, []) end,
            PoolOpts = #{timeout => 10000},

            case riak_pool:execute(Ref, Fun, PoolOpts) of
                {true, Res} ->
                    Res;
                {false, Reason} ->
                    {error, Reason}
            end;
        false ->
            {error, {badarg, WorkRef}}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) -> ok.

delete(Ref, Bucket, WorkId) when is_pid(Ref) ->
    riakc_pb_socket:delete(Ref, Bucket, WorkId);

delete(Ref, Bucket, WorkId) ->
    Fun = fun(Pid) ->
        riakc_pb_socket:delete(Pid, Bucket, WorkId)
    end,

    PoolOpts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {true, Res} ->
            Res;
        {false, Reason} ->
            {error, Reason}
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()]) -> ok.

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
        {true, Res} ->
            Res;
        {false, Reason} ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id(),
    NewItems :: [reliable:task()]) -> ok | {error, any()}.

update(Ref, Bucket, WorkId, WorkItems) ->
    Fun = fun(Pid) ->
        case riakc_pb_socket:get(Pid, Bucket, WorkId) of
            {ok, Obj0} ->
                Obj1 = riakc_obj:update_value(Obj0, WorkItems),
                case riakc_pb_socket:put(Pid, Obj1, [return_body]) of
                    {ok, _Obj2} ->
                        ok;
                    {error, Reason} ->
                        ?LOG_ERROR("failed to update object: ~p", [Reason]),
                        {error, Reason}
                end;
            {error, Reason} ->
                ?LOG_ERROR(
                    "~p: failed to read object before update: ~p",
                    [?MODULE, Reason]
                ),
                {error, Reason}
        end
    end,

    PoolOpts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {true, Res} ->
            Res;
        {false, Reason} ->
            {error, Reason}
    end.




%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------


-spec list(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) ->
    List :: {[reliable_work:t()], Continuation :: any()}.

list(Ref, Bucket, Opts) ->
    {L, Cont} = fold(
        Ref, Bucket, fun(Work, Acc) -> [Work | Acc] end, [], Opts
    ),
    {lists:reverse(L), Cont}.


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
    {NewAcc :: any(), Continuation :: any()}.

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

        #index_results_v1{keys = Keys, continuation = Cont1} = maybe_error(Res),

        ?LOG_DEBUG("Got work keys: ~p", [Keys]),

        FoldFun = fun(Key, Acc1) ->
            case get_work(Pid, Bucket, Key, []) of
                {ok, Work} ->
                    ?LOG_DEBUG("got key: ~p", [Key]),
                    ?LOG_DEBUG("got work: ~p", [Work]),
                    Function({Key, Work}, Acc1);
                {error, Reason} ->
                    ?LOG_ERROR(
                        "Error while retrieving work from store; "
                        "backend=~p, reason=~p, partition=~p, key=~p",
                        [?MODULE, Reason, Bucket, Key]
                    ),
                    Acc1
            end
        end,
        Result = lists:foldl(FoldFun, Acc, Keys),
        {Result, Cont1}
    end,

    PoolOpts = #{timeout => 10000},

    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {true, Res} ->
            Res;
        {false, Reason} ->
            ?LOG_INFO(
                "Could not retrieve work from store; reason=~p",
                [Reason]
            ),
            {[], undefined}
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



maybe_error({ok, Result}) -> Result;
maybe_error({error, "overload"}) -> error(overload);
maybe_error({error, Reason}) -> error(Reason).


%% @private
get_work(Ref, Bucket, WorkId, RiakOpts) ->
    case riakc_pb_socket:get(Ref, Bucket, WorkId, RiakOpts) of
        {ok, Object} ->
            Data = riakc_obj:get_value(Object),
            safe_decode_work(Ref, Bucket, WorkId, Data);
        {error, notfound} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.


%% @private
safe_decode_work(Ref, Bucket, WorkId, Data) ->
    Term = binary_to_term(Data),
    case reliable_work:is_type(Term) of
        true ->
            {ok, Term};
        false ->
            ?LOG_ERROR(#{
                message =>
                    "Found invalid work term in store. "
                    "Work will be deleted.",
                bucket => bucket,
                work_id => WorkId,
                term => Term
            }),
            ok = delete(Ref, Bucket, WorkId),
            {error, invalid_term}
    end.