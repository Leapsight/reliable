%% =============================================================================
%%  reliable_store_backend.erl -
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

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(reliable_store_backend).


-type ref()        ::   pid() | reference() | atom().

-export_type([ref/0]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================



-callback init() -> {ok, ref()} | {error, any()}.


-callback enqueue(
    Ref :: ref(),
    Bucket :: binary(),
    Work :: reliable_work:t()) ->
    ok | {error, any()}.


-callback update(
    Ref :: ref(),
    Bucket :: binary(),
    Work :: reliable_work:t()) -> ok | {error, any()}.


-callback fold(
    Ref :: ref(),
    Bucket :: binary(),
    Fun :: function(),
    Acc :: any(),
    Opts :: map()) ->
    {NewAcc :: any(), Continuation :: any()}.


-callback list(
    Ref :: ref(),
    Bucket :: binary(),
    Opts :: map()) ->
    List :: {[reliable_work:t()], Continuation :: any()}.


-callback flush(Ref :: ref(), Bucket :: binary()) -> ok | {error, any()}.


-callback get(Ref :: ref(), WorkRef :: reliable_work_ref:t()) ->
    {ok, term()} | {error, not_found | any()}.


-callback delete(
    Ref :: ref(),
    Bucket :: binary(),
    WorkId :: reliable_work:id()) -> ok.


-callback delete_all(
    Ref :: ref(),
    Bucket :: binary(),
    AllCompleted :: [reliable_work:id()]) -> ok.