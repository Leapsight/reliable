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

-type opts()    :: optional(riak_pool:exec_opts()).

-export([count/3]).
-export([delete/4]).
-export([delete_all/4]).
-export([enqueue/4]).
-export([flush/3]).
-export([fold/5]).
-export([get/4]).
-export([init/0]).
-export([list/3]).
-export([update/4]).


-dialyzer([{nowarn_function, count/3}]).
-dialyzer([{nowarn_function, delete_all/4}]).
-dialyzer([{nowarn_function, enqueue/4}]).
-dialyzer([{nowarn_function, fold/5}]).
-dialyzer([{nowarn_function, get/4}]).
-dialyzer([{nowarn_function, update/4}]).


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
                description => "Error while getting db connection from pool.",
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
    Work :: reliable_work:t(),
    PoolOpts :: opts()) -> ok | {error, any()} | no_return().

enqueue(Ref, Bucket, Work, undefined) ->
    enqueue(Ref, Bucket, Work, #{timeout => 15000});

enqueue(Ref, Bucket, Work, Opts) when is_map(Opts) ->
    WorkId = reliable_work:id(Work),

    Fun = fun(Pid) ->
        Object = riakc_obj:new(Bucket, WorkId, term_to_binary(Work)),

        %% eqwalizer:ignore Object
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
                maybe_throw(reliable_riak_util:format_error_reason(Reason))
        end
    end,

    %% eqwalizer:ignore Ref
    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            %% eqwalizer:ignore Result
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
    WorkId :: reliable_work:id(),
    Opts :: opts()) ->
    {ok, reliable_work:t()} | {error, not_found | any()} | no_return().

get(Ref, Bucket, WorkId, undefined) ->
    get(Ref, Bucket, WorkId, #{timeout => ?DEFAULT_TIMEOUT});

get(Ref, Bucket, WorkId, Opts) when is_map(Opts) ->
    Fun = fun(Pid) ->
        get_work(Pid, Bucket, WorkId, [])
    end,

    %% eqwalizer:ignore Ref
    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            %% eqwalizer:ignore Result
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
    WorkId :: reliable_work:id(),
    Opts :: opts()) ->
    ok
    | {error, too_many_fails}
    | {error, not_found}
    | {error, timeout}
    | {error, {n_val_violation, N :: integer()}}
    | {error, term()}
    | no_return().

delete(Pid, Bucket, WorkId, undefined) ->
    delete(Pid, Bucket, WorkId, #{timeout => 10000});

delete(Pid, Bucket, WorkId, Opts) when is_pid(Pid), is_map(Opts) ->
    case riakc_pb_socket:delete(Pid, Bucket, WorkId) of
        ok ->
            %% Cache so that we avoid considering it if next batch
            %% includes this WorkId. This happens with the
            %% as the $bucket index is slow to get updated after a delete.
            reliable_cache:put(WorkId);
        {error, Reason} ->
            {error, reliable_riak_util:format_error_reason(Reason)}
    end;

delete(Ref, Bucket, WorkId, Opts) when is_atom(Ref), is_map(Opts) ->
    Fun = fun(Pid) ->
        case delete(Pid, Bucket, WorkId, Opts) of
            ok ->
                ok;
            {error, Reason} ->
                maybe_throw(Reason)
        end
    end,

    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            %% eqwalizer:ignore Result
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
    AllCompleted :: [reliable_work:id()],
    Opts :: opts()) -> ok | {error, Reason :: any()} | no_return().

delete_all(Ref, Bucket, WorkIds, undefined) ->
    delete_all(Ref, Bucket, WorkIds, #{timeout => ?DEFAULT_TIMEOUT});

delete_all(Ref, Bucket, WorkIds, Opts) ->
    Fun = fun(Pid) ->
        _ = lists:foreach(
            fun(WorkId) ->
                case delete(Pid, Bucket, WorkId, Opts) of
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

    %% eqwalizer:ignore Ref
    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            %% eqwalizer:ignore Result
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
    Work :: reliable_work:t(),
    Opts :: opts()) ->
    ok
    | {error, too_many_fails}
    | {error, timeout}
    | {error, {n_val_violation, N :: integer()}}
    | {error, term()}
    | no_return().

update(Ref, Bucket, Work, undefined) ->
    update(Ref, Bucket, Work, #{timeout => ?DEFAULT_TIMEOUT});

update(Ref, Bucket, Work, Opts) ->
    WorkId = reliable_work:id(Work),

    Fun = fun(Pid) ->
        case riakc_pb_socket:get(Pid, Bucket, WorkId) of
            {ok, Obj} ->
                do_update(Pid, Bucket, Work, Obj);

            {error, Reason} ->
                ?LOG_ERROR(#{
                    description => "failed to read object before update.",
                    work_id => WorkId,
                    bucket => Bucket,
                    reason => Reason
                }),
                {error, reliable_riak_util:format_error_reason(Reason)}
        end
    end,

    %% eqwalizer:ignore Ref
    case riak_pool:execute(Ref, Fun, Opts) of
        {ok, Result} ->
            %% eqwalizer:ignore Result
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
    Opts :: opts()) ->
    {ok, Count :: integer()} | {error, Reason :: any()} | no_return().

count(Ref, Bucket, undefined) ->
    count(Ref, Bucket, #{});

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
                {ok, Count};

            {error, Reason} ->
                {error, reliable_riak_util:format_error_reason(Reason)}
        end
    end,

    %% eqwalizer:ignore Ref
    riak_pool:execute(Ref, Fun, PoolOpts).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: opts()) ->
    {ok, {[reliable_work:t()], Continuation :: continuation()}}
    | {error, Reason :: any()}.

list(Ref, Bucket, undefined) ->
    list(Ref, Bucket, #{});

list(Ref, Bucket, Opts) ->
    Fun = fun
        ({_K, Work}, Acc) ->
            [Work | Acc]
    end,

    try fold(Ref, Bucket, Fun, [], Opts) of
        {ok, {L, Cont}} when is_list(L) ->
            %% eqwalizer:ignore
            {ok, {lists:reverse(L), Cont}};
        {error, _} = Error ->
            Error
    catch
        throw:Reason ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: map()) -> ok | {error, any()}.

flush(Ref, Bucket, Opts) ->
    Fun = fun({K, _}, Acc) ->
        case delete(Ref, Bucket, K, #{}) of
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




%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec fold(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Fun :: function(),
    Acc :: any(),
    Opts :: opts()) ->
    {ok, {NewAcc :: any(), Continuation :: continuation()}}
    | {error, Reason :: any()}
    | no_return().

fold(Ref, Bucket, Function, Acc, undefined) ->
    fold(Ref, Bucket, Function, Acc, #{});

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
                ?LOG_DEBUG(#{
                    description => "Queue contents",
                    keys => Keys,
                    partition => Bucket
                }),

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
                            Expected =
                                Reason == not_found
                                andalso reliable_cache:has(Key),

                            case Expected of
                                true ->
                                    %% If this WorkId is cached it means we
                                    %% must have completed the
                                    %% work before and deleted the object in
                                    %% Riak KV but either the
                                    %% $bucket index hasn't been updated yet to
                                    %% reflect this or the delete failed, so we
                                    %% skip it and try to delete it again.
                                    ok = try_delete_completed(
                                        Ref, Bucket, Key
                                    );
                                false ->
                                    %% Reasons:
                                    %% - timeout
                                    %% - {n_val_violation, N::integer()}
                                    %% - {r_val_unsatisfied,
                                    %%  R::integer(), Replies::integer()}
                                    %% - term()
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
                %% eqwalizer:ignore Ref Keys
                {ok, {lists:foldl(FoldFun, Acc, Keys), Cont1}};

            {error, Reason} ->
                {error, reliable_riak_util:format_error_reason(Reason)}
        end

    end,

    PoolOpts = #{timeout => ?DEFAULT_TIMEOUT},

    %% eqwalizer:ignore Ref
    case riak_pool:execute(Ref, Fun, PoolOpts) of
        {ok, {ok, _} = OK} ->
            %% eqwalizer:ignore OK
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
when
Reason == too_many_fails orelse
Reason == disconnected orelse
Reason == overload orelse
Reason == timeout orelse
is_tuple(Reason) andalso element(1, Reason) == n_val_violation ->
    %% Throw so that riak_pool can retry according to PoolOpts
    throw(Reason);

maybe_throw(Reason) ->
    {error, Reason}.


%% @private
get_work(Ref, Bucket, WorkId, RiakOpts) ->
    case riakc_pb_socket:get(Ref, Bucket, WorkId, RiakOpts) of
        {ok, Object} ->
            object_to_work(Ref, Bucket, WorkId, Object);
        {error, Reason} ->
            {error, reliable_riak_util:format_error_reason(Reason)}
    end.


%% @private
object_to_work(Ref, Bucket, WorkId, Object) ->
    Data = riakc_obj:get_value(Object),
    Term = binary_to_term(Data),

    try
        {ok, reliable_work:from_term(Term)}
    catch
        _:Reason ->
            ?LOG_ERROR(#{
                description =>
                    "Found invalid work term in store. "
                    "Term will be deleted.",
                reason => Reason,
                bucket => bucket,
                work_id => WorkId,
                term => Term
            }),
            ok = delete(Ref, Bucket, WorkId, #{}),
            {error, invalid_term}
    end.


%% @private
do_update(Pid, Bucket, Work, Obj0) when is_pid(Pid) ->
    Obj1 = riakc_obj:update_value(Obj0, term_to_binary(Work)),

    case riakc_pb_socket:put(Pid, Obj1, [return_body]) of
        {ok, _Obj2} ->
            ok;
        {error, Reason} ->
            %% Possible reasons
            %% - notfound
            %% - too_many_fails
            %% - timeout
            %% - {n_val_violation, N :: integer()}
            %% - term()
            ?LOG_ERROR(#{
                description => "Failed to update object.",
                work_id => reliable_work:id(Work),
                bucket => Bucket,
                reason => Reason
            }),
            {error, reliable_riak_util:format_error_reason(Reason)}
    end.



%% @private
try_delete_completed(StoreRef, Bucket, WorkId) ->
    ?LOG_DEBUG(#{
        description =>
            "Work found in cache of completed work. "
            "Deleting the work in Riak must have "
            "previously failed. Trying to delete again.",
        bucket => Bucket,
        work_id => WorkId
    }),

    case delete(StoreRef, Bucket, WorkId, #{}) of
        ok ->
            ok;

        {error, not_found} ->
            %% A temporal inconsistency, not a problem
            ok;

        {error, Reason} ->
            ?LOG_ERROR(#{
                description =>
                    "Work found in cache of completed work. "
                    "Deleting the work in Riak failed.",
                reason => Reason
            }),
            ok
    end.
