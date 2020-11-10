-module(reliable_work).

-record(reliable_work, {
    id                      ::  binary(),
    tasks = #{}             ::  #{order() := reliable_task:t()},
    event_payload           ::  undefined | any()
}).


-type t()                   ::  #reliable_work{}.
-type order()               ::  pos_integer().

-export_type([t/0]).
-export_type([order/0]).


%% API
-export([add_task/3]).
-export([event_payload/1]).
-export([id/1]).
-export([is_type/1]).
-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([new/3]).
-export([ref/2]).
-export([tasks/1]).
-export([update_task/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    new(undefined).


%% -----------------------------------------------------------------------------
%% @doc Calls {@link new/3} with `[]' as the second argument and `undefined' as
%% the third argument.
%% @end
%% -----------------------------------------------------------------------------
-spec new(Id :: binary() | undefined) -> t().

new(Id) ->
    new(Id, [], undefined).


%% -----------------------------------------------------------------------------
%% @doc Calls {@link new/3} with `undefined' as the third argument.
%% @end
%% -----------------------------------------------------------------------------
-spec new(Id :: binary() | undefined, [{order(), reliable_task:t()}]) -> t().

new(Id, Tasks) ->
    new(Id, Tasks, undefined).


%% -----------------------------------------------------------------------------
%% @doc Creates a new work object with identifier `Id', tasks `Tasks' and event
%% payload `EnetPayload'.
%%
%% `Id' needs to be a key sorteable unique identifier.
%% If the atom `undefined' is passed, a global key sorteable unique identifier /
%% will be generated, using `ksuid:gen_id(millisecond)'.
%%
%% Tasks is a property lists where the key is and order number and the value an
%% instance of {@link reliable_work}.
%%
%% `EventPayload' is any data you would like the subscribers to the Reliable
%% Events to receive.
%% @end
%% -----------------------------------------------------------------------------
-spec new(
    Id :: binary() | undefined,
    Tasks :: [{order(), reliable_task:t()}],
    EventPayload :: undefined | any()) -> t().

new(undefined, Tasks, EventPayload) ->
    new(ksuid:gen_id(millisecond), Tasks, EventPayload);

new(Id, Tasks, EventPayload) when is_binary(Id) ->
    Work = #reliable_work{id = Id, event_payload = EventPayload},
    lists:foldl(
        fun({Order, Task}, Acc) -> add_task(Order, Task, Acc) end,
        Work,
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
-spec ref(StoreRef :: atom(), Work :: t()) -> reliable_work_ref:t().

ref(StoreRef, #reliable_work{id = Id}) ->
    reliable_work_ref:new(Id, StoreRef).


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
    end;

add_task(Order, _, #reliable_work{}) ->
    error({badarg, Order});

add_task(_, _, Term) ->
    error({badarg, Term}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update_task(Order :: order(), Task :: reliable_task:t(), Work :: t()) ->
    t().

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


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec event_payload(t()) -> undefined | any().

event_payload(#reliable_work{event_payload = Val}) -> Val.
