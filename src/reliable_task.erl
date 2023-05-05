%% =============================================================================
%%  reliable_partition_store.erl -
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
-module(reliable_task).

-record(reliable_task, {
    node                    ::  node(),
    module                  ::  module(),
    function                ::  atom(),
    args                    ::  [term()],
    status                  ::  completed | failed | undefined,
    timestamp               ::  integer() | undefined,
    retries = 0             ::  integer(),
    result                  ::  term() | undefined
}).


-type t()                   ::  #reliable_task{}.

-export_type([t/0]).


%% API
-export([args/1]).
-export([function/1]).
-export([has_result/1]).
-export([is_type/1]).
-export([module/1]).
-export([new/4]).
-export([node/1]).
-export([result/1]).
-export([set_result/2]).
-export([set_status/2]).
-export([status/1]).
-export([from_term/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Args :: [term()]) -> t().

new(Node, Module, Function, Args)
when is_atom(Node)
andalso is_atom(Module)
andalso is_atom(Function)
andalso is_list(Args) ->
    #reliable_task{
        node = Node,
        module = Module,
        function = Function,
        args = Args
    }.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(Task :: t()) -> boolean().

is_type(#reliable_task{}) -> true;
is_type(_) -> false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(Task :: t()) -> node().

node(#reliable_task{node = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec module(Task :: t()) -> module().

module(#reliable_task{module = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec function(Task :: t()) -> atom().

function(#reliable_task{function = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec result(Task :: t()) -> term().

result(#reliable_task{result = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec has_result(Task :: t()) -> boolean().

has_result(#reliable_task{result = Val}) ->
    Val =/= undefined.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec args(Task :: t()) -> list().

args(#reliable_task{args = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_result(Result :: any(), Task :: t()) -> NewTask :: t().

set_result(Result, #reliable_task{} = T) ->
    T#reliable_task{result = Result}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(Task :: t()) -> completed | failed | undefined.

status(#reliable_task{status = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_status(completed | failed, Task :: t()) -> NewTask :: t().

set_status(Status, #reliable_task{} = T)
when Status == completed; Status == failed ->
    T#reliable_task{status = Status}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(any()) -> t() | no_return().

from_term(#reliable_task{} = T) ->
    %% Current version
    T;

from_term({reliable_task, N, M, F, A, R})
when
is_atom(N),
is_atom(M),
is_atom(F),
is_list(A) ->
    %% Version 1.0
    Status = case R of
        undefined ->
            undefined;
        {error, _} ->
            failed;
        _ ->
            completed
    end,

    #reliable_task{
        node = N,
        module = M,
        function = F,
        args = A,
        status = Status,
        timestamp = erlang:system_time(millisecond),
        result = R
    };

from_term(Term) ->
    error({badarg, Term}).


