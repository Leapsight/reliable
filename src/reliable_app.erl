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

-module(reliable_app).

-behaviour(application).

-export([start/2,
         stop/1]).




%% =============================================================================
%% APPLICATION CALLBACKS
%% =============================================================================



start(_StartType, _StartArgs) ->
    case reliable_sup:start_link() of
        {ok, _} = OK -> OK;
        {error, _} = Error -> Error
    end.

stop(_State) ->
    ok.
