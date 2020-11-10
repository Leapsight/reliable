%% =============================================================================
%%  reliable_work_ref.erl -
%%
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
-module(reliable_work_ref).

-author("Alejandro Ramallo <alejandro.ramallo@leapsight.com>").

-record(reliable_work_ref, {
    store_ref   ::  atom(),
    work_id     ::  reliable_work:id()
}).

-type t()       ::  #reliable_work_ref{}.

-export_type([t/0]).

-export([new/2]).
-export([work_id/1]).
-export([is_type/1]).
-export([store_ref/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new(reliable_work:id(), atom()) -> t().

new(WorkId, StoreRef) when is_binary(WorkId) andalso is_atom(StoreRef) ->
    #reliable_work_ref{work_id = WorkId, store_ref = StoreRef}.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(Ref :: t()) -> boolean().

is_type(#reliable_work_ref{}) -> true;
is_type(_) -> false.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec work_id(Ref :: t()) -> binary().

work_id(#reliable_work_ref{work_id = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec store_ref(Ref :: t()) -> atom().

store_ref(#reliable_work_ref{store_ref = Val}) ->
    Val.

