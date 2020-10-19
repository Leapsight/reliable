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

-define(POOLNAME, reliable).

-export([init/0,
         enqueue/3,
         get/2,
         delete/3,
         delete_all/3,
         update/4,
         list/3,
         fold/5]).



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
            ?LOG_INFO(
                "Error while getting db connection from pool during init, will try allocation a connection directly;"
                "poolname=~p, reason=~p",
                [reliable, Reason]
            ),
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------

enqueue(Ref, Bucket, {WorkId, WorkItems}) ->
    Fun = fun(Pid) ->
        Object = riakc_obj:new(
            Bucket,
            WorkId,
            term_to_binary(WorkItems)
        ),
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
get(Ref, {work_ref, Bucket, WorkId}) ->
    Fun = fun(Pid) ->
        case riakc_pb_socket:get(Pid, Bucket, WorkId, []) of
            {ok, Object} ->
                BinaryData = riakc_obj:get_value(Object),
                TermData = binary_to_term(BinaryData),
                {ok, TermData};
            {error, notfound} ->
                {error, not_found};
            {error, _} = Error ->
                Error
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
update(Ref, Bucket, WorkId, WorkItems) ->
    Fun = fun(Pid) ->
        case riakc_pb_socket:get(Pid, Bucket, WorkId) of
            {ok, O} ->
                O1 = riakc_obj:update_value(O, WorkItems),
                case riakc_pb_socket:put(Pid, O1, [return_body]) of
                    {ok, _O2} ->
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
    Ref :: pid(),
    Bucket :: binary(),
    Opts :: map()) ->
    List :: [reliable_partition_worker:work()].

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
    Ref :: pid(),
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
            case riakc_pb_socket:get(Pid, Bucket, Key) of
                {ok, Object} ->
                    BinaryData = riakc_obj:get_value(Object),
                    TermData = binary_to_term(BinaryData),
                    ?LOG_DEBUG("got key: ~p", [Key]),
                    ?LOG_DEBUG("got term data: ~p", [TermData]),
                    Function({Key, TermData}, Acc1);
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