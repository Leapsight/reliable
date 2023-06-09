%% =============================================================================
%%  reliable_work.erl -
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
-module(reliable_work).

-record(reliable_work, {
    id                      ::  id(),
    tasks = #{}             ::  #{pos_integer() := reliable_task:t()},
    event_payload           ::  undefined | any()
}).


-type t()                   ::  #reliable_work{}.
-type id()                  ::  binary().
-type status()              ::  #{
                                    work_id := binary(),
                                    nbr_of_tasks := integer(),
                                    nbr_of_tasks_remaining := integer(),
                                    event_payload := any()
                                }.
-export_type([t/0]).
-export_type([id/0]).
-export_type([status/0]).

%% API
-export([add_task/3]).
-export([event_payload/1]).
-export([from_term/1]).
-export([id/1]).
-export([is_type/1]).
-export([nbr_of_tasks/1]).
-export([new/0]).
-export([new/1]).
-export([new/2]).
-export([new/3]).
-export([ref/2]).
-export([status/1]).
-export([tasks/1]).
-export([update_task/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Returns a new work object.
%% Calls {@link new/1} with the atom `undefined' as argument.
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
-spec new(Id :: binary() | undefined, [{pos_integer(), reliable_task:t()}]) -> t().

new(Id, Tasks) ->
    new(Id, Tasks, undefined).


%% -----------------------------------------------------------------------------
%% @doc Creates a new work object with identifier `Id', tasks `Tasks' and event
%% payload `EventPayload'.
%%
%% `Id' needs to be a key sorteable unique identifier.
%% If the atom `undefined' is passed, a global key sorteable unique identifier /
%% will be generated, using `ksuid:gen_id(millisecond)'.
%%
%% `Tasks' is a property lists where the key is and order number and the value
%% is a task (See {@link reliable_task:t()}).
%%
%% `EventPayload' is any data you would like the subscribers to the Reliable
%% Events to receive.
%% @end
%% -----------------------------------------------------------------------------
-spec new(
    Id :: binary() | undefined,
    Tasks :: [{pos_integer(), reliable_task:t()}],
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
%% @doc Returns `true' if `Arg' is a work object. Otherwise, returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_type(Arg :: term()) -> boolean().

is_type(#reliable_work{}) -> true;
is_type(_) -> false.


%% -----------------------------------------------------------------------------
%% @doc Returns the `id' of a work object.
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
-spec add_task(
    Order :: pos_integer(), Task :: reliable_task:t(), Work :: t()) ->
    t() | no_return().

add_task(Order, Task0, #reliable_work{tasks = Tasks} = Work)
when is_integer(Order) andalso Order > 0 ->
    %% Upgrade task
    Task =
        case reliable_task:is_type(Task0) of
            true ->
                Task0;
            false ->
                reliable_task:from_term(Task0)
        end,

    Work#reliable_work{
        tasks = maps:put(Order, Task, Tasks)
    };

add_task(Order, _, #reliable_work{}) ->
    error({badarg, Order});

add_task(_, _, Term) ->
    error({badarg, Term}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec update_task(Order :: pos_integer(), Task :: reliable_task:t(), Work :: t()) ->
    t() | no_return().

update_task(Order, Task0, #reliable_work{tasks = Tasks} = Work)
when is_integer(Order) andalso Order > 0 ->
    %% Upgrade task
    Task =
        case reliable_task:is_type(Task0) of
            true ->
                Task0;
            false ->
                reliable_task:from_term(Task0)
        end,

    Work#reliable_work{
        tasks = maps:update(Order, Task, Tasks)
    };

update_task(Order, _, #reliable_work{}) ->
    error({badarg, Order});

update_task(_, _, Term) ->
    error({badarg, Term}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec tasks(t()) -> [{pos_integer(), reliable_task:t()}].

tasks(#reliable_work{tasks = Val}) ->
    maps:to_list(Val).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec nbr_of_tasks(t()) -> integer().

nbr_of_tasks(#reliable_work{tasks = Val}) ->
    maps:size(Val).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec event_payload(t()) -> undefined | any().

event_payload(#reliable_work{event_payload = Val}) -> Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(t()) -> status().

status(#reliable_work{id = Id, tasks = Tasks, event_payload = Payload}) ->
    Remaining = maps:fold(
        fun(_, Task, Acc) ->
            case reliable_task:status(Task) of
                completed ->
                    Acc;
                Status when Status == undefined; Status == failed ->
                    Acc + 1
            end
        end,
        0,
        Tasks
    ),
    #{
        work_id => Id,
        nbr_of_tasks => maps:size(Tasks),
        nbr_of_tasks_remaining => Remaining,
        event_payload => Payload
    }.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec from_term(any()) -> t() | no_return().

from_term(#reliable_work{tasks = Tasks0} = Work) ->
    Tasks = maps:map(
        fun(_, V) ->
            reliable_task:from_term(V)
        end,
        Tasks0
    ),
    Work#reliable_work{tasks = Tasks}.




