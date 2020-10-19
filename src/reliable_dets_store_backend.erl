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
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-define(TABLE, reliable_backend).
-define(FILENAME, "reliable-backend-data").

-export([init/0,
         enqueue/2,
         delete_all/2,
         update/3,
         fold/3]).

init() ->
    case dets:open_file(?TABLE, [{file, ?FILENAME}]) of
        {ok, Reference} ->
            {ok, Reference};
        {error, Reason} ->
            {error, Reason}
    end.

enqueue(Reference, Work) ->
    case dets:insert_new(Reference, Work) of
        true ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

delete_all(Reference, WorkIds) ->
    lists:foreach(fun(Key) -> dets:delete(Reference, Key) end, WorkIds),
    ok.

update(Reference, WorkId, WorkItems) ->
    dets:insert(Reference, {WorkId, WorkItems}).

fold(Reference, Function, Acc) ->
    dets:foldl(Function, Acc, Reference).