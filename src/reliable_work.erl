-module(reliable_work).

-record(reliable_work, {
    id                      ::  binary(),
    tasks = #{}             ::  #{order() := reliable_task:t()}
}).


-type t()                   ::  #reliable_work{}.
-type order()               ::  pos_integer().

-export_type([t/0]).
-export_type([order/0]).


%% API
-export([add_task/3]).
-export([is_type/1]).
-export([id/1]).
-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([tasks/1]).
-export([ref/2]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    new(ksuid:gen_id(millisecond)).


%% -----------------------------------------------------------------------------
%% @doc
%% `Id' needs to be a key sortable unique term identifier
%% @end
%% -----------------------------------------------------------------------------
-spec new(Id :: binary() | undefined) -> t().

new(undefined) ->
    new();

new(Id) when is_binary(Id) ->
    #reliable_work{id = Id}.


%% -----------------------------------------------------------------------------
%% @doc
%% `Id' needs to be a key sortable unique term identifier
%% @end
%% -----------------------------------------------------------------------------
-spec new(Id :: binary() | undefined, [{order(), reliable_task:t()}]) -> t().

new(Id, Tasks) ->
    lists:foldl(
        fun({Order, Task}, Acc) -> add_task(Order, Task, Acc) end,
        new(Id),
        Tasks
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(Work :: t()) -> boolean().

is_type(#reliable_work{}) -> true;
is_type(_) -> false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec id(Work :: t()) -> binary().

id(#reliable_work{id = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec ref(Instance :: binary(), Work :: t()) -> reliable_work_ref:t().

ref(Instance, #reliable_work{id = Id}) ->
    reliable_work_ref:new(Id, Instance).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_task(Order :: order(), Task :: reliable_task:t(), Work :: t()) -> t().

add_task(Order, Task, #reliable_work{tasks = Tasks} = Work)
when is_integer(Order) andalso Order > 0 ->
    case reliable_task:is_type(Task) of
        true ->
            Work#reliable_work{
                tasks = maps:put(Order, Task, Tasks)
            };
        false ->
            error({badarg, Task})
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update_task(Order :: order(), Task :: reliable_task:t(), Work :: t()) -> t().

update_task(Order, Task, #reliable_work{tasks = Tasks} = Work)
when is_integer(Order) andalso Order > 0 ->
    case reliable_task:is_type(Task) of
        true ->
            Work#reliable_work{
                tasks = maps:update(Order, Task, Tasks)
            };
        false ->
            error({badarg, Task})
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec tasks(t()) -> [{order(), reliable_task:t()}].

tasks(#reliable_work{tasks = Val}) ->
    maps:to_list(Val).