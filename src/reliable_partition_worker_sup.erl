%% =============================================================================
%%  reliable_partition_worker_sup.erl -
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

-module(reliable_partition_worker_sup).
-behaviour(supervisor).


%% API
-export([start_link/0]).
-export([init/1]).



%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).



%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================



init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },

    %% We spawn a child per store partition. Each server is the single writer
    %% to that store partition, the store partition acting as a queue.
    ChildSpecs = [
        begin
            StoreName = binary_to_atom(Bucket, utf8),
            Name = binary_to_atom(<<Bucket/binary, "_worker">>, utf8),
            #{
                id => Name,
                start => {
                    reliable_partition_worker,
                    start_link,
                    [Name, StoreName, Bucket]
                },
                restart => permanent,
                shutdown => infinity,
                type => worker,
                modules => [reliable_partition_worker]
            }
        end || Bucket <- reliable_config:local_partitions()
    ],

    {ok, {SupFlags, ChildSpecs}}.
