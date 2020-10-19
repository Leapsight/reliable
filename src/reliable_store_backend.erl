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



%% =============================================================================
%% CALLBACKS
%% =============================================================================




-callback init() -> {ok, pid()} | {error, any()}.


-callback enqueue(
    Ref :: pid(), Bucket :: binary(), Work :: reliable_partition_worker:work()) ->
    ok | {error, any()}.


-callback update(
    Ref :: pid(),
    Bucket :: binary(),
    WorkId :: reliable_partition_worker:work_id(),
    NewItems :: [reliable_partition_worker:work_item()]) -> ok | {error, any()}.


-callback fold(
    Ref :: pid(),
    Bucket :: binary(),
    Fun :: function(),
    Acc :: any(),
    Opts :: map()) ->
    {NewAcc :: any(), Continuation :: any()}.

-callback list(
    Ref :: pid(),
    Bucket :: binary(),
    Opts :: map()) ->
    List :: {[reliable_partition_worker:work()], Continuation :: any()}.

-callback get(
    Ref :: pid(),
    WorkRef :: reliable_partition_worker:work_ref()) ->
        {ok, term()} | {error, not_found | any()}.


-callback delete(
    Ref :: pid(),
    Bucket :: binary(),
    WorkId :: reliable_partition_worker:work_id()) -> ok.


-callback delete_all(
    Ref :: pid(),
    Bucket :: binary(),
    AllCompleted :: [reliable_partition_worker:work_id()]) -> ok.