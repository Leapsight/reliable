%% =============================================================================
%%  reliable_sup.erl -
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

-module(reliable_sup).
-behaviour(supervisor).

-define(SUPERVISOR(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => supervisor,
    modules => [Id]
}).

-define(WORKER(Id, Args, Restart, Timeout),
    ?WORKER(Id, Id, Args, Restart, Timeout)
).

-define(WORKER(Id, Mod, Args, Restart, Timeout), #{
    id => Id,
    start => {Mod, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [Id]
}).

-define(EVENT_MANAGER(Id, Restart, Timeout), #{
    id => Id,
    start => {gen_event, start_link, [{local, Id}]},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [dynamic]
}).


%% API
-export([start_link/0]).
-export([init/1]).



%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    try
        ok = reliable_config:init(),
        ok = add_riak_pool(),
        supervisor:start_link({local, ?MODULE}, ?MODULE, [])
    catch
        _:Reason ->
            {error, Reason}
    end.



%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================



init([]) ->

    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 5
    },

    CacheOpts = [{n, 5}, {ttl, 60}],

    ChildSpecs = [
        ?SUPERVISOR(
            reliable_event_handler_watcher_sup, [], permanent, infinity
        ),
        ?WORKER(reliable_event_manager, [], permanent, 5000),
        %% ?EVENT_MANAGER(reliable_event_manager, permanent, 5000),
        %% Start partition stores before workers
        ?SUPERVISOR(reliable_partition_store_sup, [], permanent, infinity),
        ?SUPERVISOR(reliable_partition_worker_sup, [], permanent, infinity),
        ?WORKER(
            reliable_cache, cache, [reliable_cache, CacheOpts], permanent, 5000
        )
    ],

    {ok, {SupFlags, ChildSpecs}}.




%% =============================================================================
%% PRIVATE
%% =============================================================================



add_riak_pool() ->
    %% Remove the pool just in case it exists as we want to use the latest
    %% config.
    ok = riak_pool:remove_pool(reliable),

    Config = case reliable_config:get(riak_pool, undefined) of
        undefined ->
            Size = length(reliable_config:instances()),
            #{
                min_size => Size,
                max_size => Size * 2
            };
        Term when is_map(Term) ->
            Term
    end,

    %% eqwalizer:ignore Config
    case riak_pool:add_pool(reliable, Config) of
        ok ->
            ok;

        {error, Reason} ->
            throw(Reason)
    end.
