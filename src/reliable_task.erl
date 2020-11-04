-module(reliable_task).

-record(reliable_task, {
    node                    ::  node(),
    module                  ::  module(),
    function                ::  atom(),
    args                    ::  [term()],
    result                  ::  term() | undefined
}).


-type t()                   ::  #reliable_task{}.

-export_type([t/0]).


%% API
-export([new/4]).
-export([node/1]).
-export([module/1]).
-export([function/1]).
-export([args/1]).
-export([is_type/1]).
-export([result/1]).
-export([set_result/2]).



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
        args = Args,
        result = undefined
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
-spec args(Task :: t()) -> term().

args(#reliable_task{args = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_result(Result :: any(), Task :: t()) -> NewTask :: t().

set_result(Result, #reliable_task{} = T) ->
    T#reliable_task{result = Result}.
