%% =============================================================================
%%  reliable_dets_store_backend.erl -
%%
%%  Copyright (c) 2020 Christopher Meiklejohn. All rights reserved.
%%  Copyright (c) 2016-2019 Leapsight Holdings Limited. All rights reserved.
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

-module(reliable_dets_store_backend).
-behaviour(reliable_store_backend).

-include_lib("kernel/include/logger.hrl").
-include("reliable.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-define(TABLE, reliable_dets).
-define(FILENAME, "reliable_dets_data").

-type continuation()     :: map() | '$end_of_table'.

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




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init() ->
    Opts = [
        {file, ?FILENAME},
        {keypos, 2},
        {auto_save, timer:seconds(60)}
    ],
    case dets:open_file(?TABLE, Opts) of
        {ok, Ref} ->
            {ok, Ref};
        {error, Reason} ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: undefined) ->
    ok | {error, Reason :: reliable_store_backend:error_reason()}.

enqueue(Ref, _Bucket, Work, _Opts) ->
    case dets:insert_new(Ref, Work) of
        true ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id(),
    Opts :: undefined) ->
    {ok, term()} | {error, not_found | any()}.

get(Ref, _Bucket, WorkId, _) ->
    case dets:lookup(Ref, WorkId) of
        [Work] ->
            {ok, Work};

        [Work|_] ->
            ?LOG_WARNING(#{
                description => "Found duplicates",
                work_id => WorkId
            }),
            {ok, Work};

        [] ->
            {error, not_found};

        {error, _} = Error ->
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
    Opts :: optional(map())) -> ok | {error, Reason :: any()}.

delete(Ref, _Bucket, WorkId, _) ->
    dets:delete(Ref, WorkId).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec delete_all(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()],
    Opts :: optional(map())) -> ok | {error, Reason :: any()}.

delete_all(Ref, _Bucket, WorkIds, _) ->
    lists:foreach(fun(Key) -> dets:delete(Ref, Key) end, WorkIds),
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: map()) -> ok | {error, any()}.

update(Ref, _Bucket, Work, _Opts) ->
    dets:insert(Ref, Work).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: optional(map())) -> {ok, Count :: integer()} | {error, Reason :: any()}.

count(Ref, _Bucket, _Opts) ->
    case dets:info(Ref) of
        undefined ->
            {error, badref};
        L ->
            {size, N} = lists:keyfind(size, 1, L),
            {ok, N}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec list(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: optional(map())) ->
    {ok, {[reliable_work:t()], Continuation :: continuation()}}
    | {error, Reason :: any()}.

list(Ref, Bucket, undefined) ->
    list(Ref, Bucket, #{});

list(Ref, Bucket, Opts) ->
    Fun = fun({_K, V}, Acc) -> [V | Acc] end,

    case fold(Ref, Bucket, Fun, [], Opts) of
        {ok, {L, Cont}} when is_list(L) ->
            %% eqwalizer:ignore L
            {ok, {lists:reverse(L), Cont}};

        {error, _} = Error ->
            Error
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
    Opts :: optional(map())) ->
    {ok, {NewAcc :: any(), Continuation :: continuation()}}
    | {error, Reason :: any()}.

fold(Ref, _Bucket, Function, Acc, undefined) ->
    fold(Ref, _Bucket, Function, Acc, #{});

fold(Ref, _Bucket, Function, Acc0, Opts) ->
    {max_results, Limit} = lists:keyfind(max_results, 1, fold_opts(Opts)),

    case Limit of
        Limit when is_integer(Limit) ->
            fold(Ref, Function, Acc0, 0, Limit, dets:first(Ref));

        infinity ->
            case dets:foldl(Function, Acc0, Ref) of
                {error, _} = Error ->
                    Error;
                Acc ->
                    {ok, {Acc, '$end_of_table'}}
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec flush(
    Ref :: reliable_store_backend:ref(),
    Bucket :: binary(),
    Opts :: optional(map())) -> ok | {error, any()}.

flush(Ref, Bucket, undefined) ->
    flush(Ref, Bucket, #{});

flush(Ref, Bucket, Opts) when is_map(Opts) ->
    Fun = fun({K, _}, Acc) ->
        case delete(Ref, Bucket, K, undefined) of
            ok ->
                Acc;
            {error, not_found} ->
                Acc;
            {error, Reason} ->
                throw(Reason)
        end
    end,

    try fold(Ref, Bucket, Fun, ok, Opts) of
        {ok, {_, '$end_of_table'}} ->
            ok;
        {ok, {_, Cont}} ->
            flush(Ref, Bucket, Opts#{continuation => Cont})
    catch
        throw:Reason ->
            {error, Reason}
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
-spec fold_opts(optional(map())) -> list().

fold_opts(undefined) ->
    fold_opts(#{});

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
fold(_, _, Acc, _, _, '$end_of_table') ->
    {ok, {Acc, '$end_of_table'}};

fold(Ref, Function, Acc, Cnt, Limit, Key)
when is_integer(Limit), Cnt == Limit ->
    Continuation = #{
        ref => Ref,
        function => Function,
        limit => Limit,
        next => Key
    },
    {ok, {Acc, Continuation}};

fold(Ref, Function, Acc0, Cnt, Limit, Key) ->
    Acc =
        case dets:lookup(Ref, Key) of
            [Work|_] ->
                Function({Key, Work}, Acc0);
            _ ->
                Acc0
        end,
    fold(Ref, Function, Acc, Cnt + 1, Limit, dets:next(Ref, Key)).




