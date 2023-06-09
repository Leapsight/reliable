%% =============================================================================
%%  reliable_store_backend.erl -
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

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(reliable_store_backend).

-include("reliable.hrl").

-type ref()             ::  pid() | reference() | atom().
-type opts()            ::  optional(any()).
-type error_reason()    ::  busy | disconnected | timeout
                            | invalid_datatype | any().

-export_type([ref/0]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================



-callback init() -> {ok, ref()} | {error, Reason :: any()}.

-callback enqueue(
    Ref :: ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: opts()) ->
    ok | {error, Reason :: error_reason()}.

-callback get(
    Ref :: ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id(),
    Opts :: opts()) ->
    {ok, term()} | {error, not_found | any()}.

-callback delete(
    Ref :: ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id(),
    Opts :: opts()) -> ok | {error, Reason :: any()}.

-callback delete_all(
    Ref :: ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()],
    Opts :: opts()) -> ok | {error, Reason :: any()}.

-callback update(
    Ref :: ref(),
    Bucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: opts()) -> ok | {error, Reason :: error_reason()}.


-callback move(
    Ref :: ref(),
    Bucket :: binary(),
    NewBucket :: binary(),
    Work :: reliable_work:t(),
    Opts :: opts()) -> ok | {error, Reason :: any()}.


-callback count(Ref :: ref(), Bucket :: binary(), Opts :: opts()) ->
    {ok, Count :: integer()} | {error, Reason :: any()}.

-callback list(
    Ref :: ref(),
    Bucket :: binary(),
    Opts :: opts()) ->
    {ok, {[reliable_work:t()], Continuation :: any()}}
    | {error, Reason :: any()}.


-callback flush(Ref :: ref(), Bucket :: binary(), Opts :: opts()) ->
    ok | {error, Reason :: any()}.


-callback fold(
    Ref :: ref(),
    Bucket :: binary(),
    Fun :: function(),
    Acc :: any(),
    Opts :: opts()) ->
    {ok, {NewAcc :: any(), Continuation :: any()}} | {error, Reason :: any()}.

