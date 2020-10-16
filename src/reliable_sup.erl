%% =============================================================================
%%  reliable_sup.erl -
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

-define(WORKER(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
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
    try reliable_config:init() of
        ok ->
        supervisor:start_link({local, ?MODULE}, ?MODULE, [])
    catch
        error:Reason ->
            {error, Reason}
    end.



%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================



init([]) ->

    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },

    ChildSpecs = [
        ?SUPERVISOR(
            reliable_event_handler_watcher_sup, [], permanent, infinity
        ),
        ?EVENT_MANAGER(reliable_event_manager, permanent, 5000),
        ?SUPERVISOR(reliable_worker_sup, [], permanent, infinity)
    ],

    {ok, {SupFlags, ChildSpecs}}.




