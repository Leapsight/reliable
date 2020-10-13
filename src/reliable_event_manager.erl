%% =============================================================================
%%  reliable_event_manager.erl -
%%
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

%% -----------------------------------------------------------------------------
%% @doc This module is used by Reliable to manage event handlers and notify
%% them of events. It implements the "watched" handler capability i.e.
%% `add_watched_handler/2,3', `swap_watched_handler/2,3'.
%% In addition, this module mirrors most of the gen_event API and adds variants
%% with two arguments were the first argument is the defaul event manager
%% (`reliable_event_manager').
%%
%% ```
%%      +---------------------------------------+
%%      |                                       |
%%      |        reliable_event_manager         |
%%      |                                       |
%%      +---------------------------------------+
%%                          |
%%                          |
%%                          v
%%      +---------------------------------------+
%%      |                                       |
%%      |  reliable_event_handler_watcher_sup   |
%%      |                                       |
%%      +---------------------------------------+
%%                          |
%%                          +--------------------------------+
%%                          |                                |
%%       +---------------------------------------+       +---+---+
%%       |                                       |       |       |
%%       |    reliable_event_handler_watcher 1   |       |   N   |
%%       |                                       |       |       |
%%       +---------------------------------------+       +-------+
%%
%%                       simple_one_for_one
%% '''
%%
%% @end
%% -----------------------------------------------------------------------------
-module(reliable_event_manager).
-behaviour(gen_event).


-record(state, {
    callback    :: function() | undefined
}).


%% API
-export([add_callback/1]).
-export([add_handler/2]).
-export([add_handler/3]).
-export([add_sup_callback/1]).
-export([add_sup_handler/2]).
-export([add_sup_handler/3]).
-export([add_watched_handler/2]).
-export([add_watched_handler/3]).
-export([notify/1]).
-export([notify/2]).
-export([subscribe/1]).
-export([subscribe/2]).
-export([swap_handler/2]).
-export([swap_handler/3]).
-export([swap_sup_handler/2]).
-export([swap_sup_handler/3]).
-export([swap_watched_handler/2]).
-export([swap_watched_handler/3]).
-export([sync_notify/1]).
-export([sync_notify/2]).
-export([unsubscribe/1]).
-export([start_link/0]).

%% gen_event callbacks
-export([init/1]).
-export([handle_event/2]).
-export([handle_call/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%% =============================================================================
%% API
%% =============================================================================


start_link() ->
    case gen_event:start_link({local, ?MODULE}) of
        {ok, _} = OK ->
            %% Ads this module as an event handler.
            %% This is to implement the pubsub capabilities.
            ok = gen_event:add_handler(?MODULE, {?MODULE, pubsub}, [pubsub]),
            OK;
        Error ->
            Error
    end.




%% -----------------------------------------------------------------------------
%% @doc Adds a callback function.
%% The function needs to have a single argument representing the event that has
%% been fired.
%% @end
%% -----------------------------------------------------------------------------
-spec add_callback(fun((any()) -> any())) -> {ok, reference()}.

add_callback(Fn) when is_function(Fn, 1) ->
    Ref = make_ref(),
    gen_event:add_handler(?MODULE, {?MODULE, Ref}, [Fn]),
    {ok, Ref}.


%% -----------------------------------------------------------------------------
%% @doc Adds a supervised callback function.
%% The function needs to have a single argument representing the event that has
%% been fired.
%% @end
%% -----------------------------------------------------------------------------
-spec add_sup_callback(fun((any()) -> any())) -> {ok, reference()}.

add_sup_callback(Fn) when is_function(Fn, 1) ->
    Ref = make_ref(),
    gen_event:add_sup_handler(?MODULE, {?MODULE, Ref}, [Fn]),
    {ok, Ref}.

%% -----------------------------------------------------------------------------
%% @doc Adds an event handler.
%% Calls `gen_event:add_handler(?MODULE, Handler, Args)'.
%% @end
%% -----------------------------------------------------------------------------
add_handler(Handler, Args) ->
    add_handler(?MODULE, Handler, Args).


%% -----------------------------------------------------------------------------
%% @doc Adds an event handler.
%% Calls `gen_event:add_handler(Manager, Handler, Args)'.
%% @end
%% -----------------------------------------------------------------------------
add_handler(Manager, Handler, Args) ->
    gen_event:add_handler(Manager, Handler, Args).



%% -----------------------------------------------------------------------------
%% @doc Adds a supervised event handler.
%% Calls `gen_event:add_sup_handler(?MODULE, Handler, Args)'.
%% @end
%% -----------------------------------------------------------------------------
add_sup_handler(Handler, Args) ->
    add_sup_handler(?MODULE, Handler, Args).

%% -----------------------------------------------------------------------------
%% @doc Adds a supervised event handler.
%% Calls `gen_event:add_sup_handler(?MODULE, Handler, Args)'.
%% @end
%% -----------------------------------------------------------------------------
add_sup_handler(Manager, Handler, Args) ->
    gen_event:add_sup_handler(Manager, Handler, Args).


%% -----------------------------------------------------------------------------
%% @doc Adds a watched event handler.
%% As opposed to `add_sup_handler/2' which monitors the caller, this function
%% calls `reliable_event_handler_watcher_sup:start_watcher(Handler, Args)' which
%% spawns a supervised process (`reliable_event_handler_watcher') which calls
%% `add_sup_handler/2'. If the handler crashes, `reliable_event_handler_watcher'
%% will re-install it in the event manager.
%% @end
%% -----------------------------------------------------------------------------
add_watched_handler(Handler, Args) ->
    add_watched_handler(?MODULE, Handler, Args).


%% -----------------------------------------------------------------------------
%% @doc Adds a supervised event handler.
%% As opposed to `add_sup_handler/2' which monitors the caller, this function
%% calls `reliable_event_handler_watcher_sup:start_watcher(Handler, Args)' which
%% spawns a supervised process (`reliable_event_handler_watcher') which calls
%% `add_sup_handler/2'. If the handler crashes, `reliable_event_handler_watcher'
%% will re-install it in the event manager.
%% @end
%% -----------------------------------------------------------------------------
add_watched_handler(Manager, Handler, Args) ->
    reliable_event_handler_watcher_sup:start_watcher(Manager, Handler, Args).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `swap_handler(reliable_event_manager, OldHandler, NewHandler)'
%% @end
%% -----------------------------------------------------------------------------
swap_handler(OldHandler, NewHandler) ->
    swap_handler(?MODULE, OldHandler, NewHandler).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling `gen_event:swap_handler/3'
%% @end
%% -----------------------------------------------------------------------------
swap_handler(Manager, {_, _} = OldHandler, {_, _} = NewHandler) ->
    gen_event:swap_handler(Manager, OldHandler, NewHandler).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `swap_sup_handler(reliable_event_manager, OldHandler, NewHandler)'
%% @end
%% -----------------------------------------------------------------------------
swap_sup_handler(OldHandler, NewHandler) ->
    swap_sup_handler(?MODULE, OldHandler, NewHandler).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling `gen_event:swap_sup_handler/3'
%% @end
%% -----------------------------------------------------------------------------
swap_sup_handler(Manager, OldHandler, NewHandler) ->
    gen_event:swap_sup_handler(Manager, OldHandler, NewHandler).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `swap_watched_handler(reliable_event_manager, OldHandler, NewHandler)'
%% @end
%% -----------------------------------------------------------------------------
swap_watched_handler(OldHandler, NewHandler) ->
    swap_watched_handler(?MODULE, OldHandler, NewHandler).


%% -----------------------------------------------------------------------------
%% @doc Replaces an event handler in event manager `Manager' in the same way as
%% `swap_sup_handler/3'. However, this function
%% calls `reliable_event_handler_watcher_sup:start_watcher(Handler, Args)' which
%% spawns a supervised process (`reliable_event_handler_watcher') which is the one
%% calling calls `swap_sup_handler/2'.
%% If the handler crashes or terminates with a reason other than `normal' or
%% `shutdown', `reliable_event_handler_watcher' will re-install it in
%% the event manager.
%% @end
%% -----------------------------------------------------------------------------
swap_watched_handler(Manager, OldHandler, NewHandler) ->
    reliable_event_handler_watcher_sup:start_watcher(
        Manager, {swap, OldHandler, NewHandler}).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `notify(reliable_event_manager, Event)'
%% @end
%% -----------------------------------------------------------------------------
notify(Event) ->
    notify(?MODULE, Event).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `gen_event:notify(reliable_event_manager, Event)'
%% @end
%% -----------------------------------------------------------------------------
notify(Manager, Event) ->
    gen_event:notify(Manager, Event).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `sync_notify(reliable_event_manager, Event)'
%% @end
%% -----------------------------------------------------------------------------
sync_notify(Event) ->
    sync_notify(?MODULE, Event).


%% -----------------------------------------------------------------------------
%% @doc A util function. Equivalent to calling
%% `gen_event:sync_notify(reliable_event_manager, Event)'
%% @end
%% -----------------------------------------------------------------------------
sync_notify(Manager, Event) ->
    gen_event:sync_notify(Manager, Event).


%% -----------------------------------------------------------------------------
%% @doc Subscribe to events of type Event.
%% Any events published through update/1 will delivered to the calling process,
%% along with all other subscribers.
%% This function will raise an exception if you try to subscribe to the same
%% event twice from the same process.
%% This function uses gproc_ps:subscribe/2.
%% @end
%% -----------------------------------------------------------------------------
-spec subscribe(term()) -> ok.

subscribe(EventType) ->
    true = gproc_ps:subscribe(l, EventType),
    ok.


%% -----------------------------------------------------------------------------
%% @doc Subscribe conditionally to events of type Event.
%% This function is similar to subscribe/2, but adds a condition in the form of
%% an ets match specification.
%% The condition is tested and a message is delivered only if the condition is
%% true. Specifically, the test is:
%% `ets:match_spec_run([Msg], ets:match_spec_compile(Cond)) == [true]'
%% In other words, if the match_spec returns true for a message, that message
%% is sent to the subscriber.
%% For any other result from the match_spec, the message is not sent. `Cond ==
%% undefined' means that all messages will be delivered, which means that
%% `Cond=undefined' and `Cond=[{'_',[],[true]}]' are equivalent.
%% This function will raise an exception if you try to subscribe to the same
%% event twice from the same process.
%% This function uses `gproc_ps:subscribe_cond/2'.
%% @end
%% -----------------------------------------------------------------------------
subscribe(EventType, MatchSpec) ->
    true = gproc_ps:subscribe_cond(l, EventType, MatchSpec),
    ok.


%% -----------------------------------------------------------------------------
%% @doc Remove subscription created using `subscribe/1,2'
%% @end
%% -----------------------------------------------------------------------------
unsubscribe(EventType) ->
    true = gproc_ps:unsubscribe(l, EventType),
    ok.



%% =============================================================================
%% GEN_EVENT CALLBACKS
%% This is to support adding a fun via add_callback/1 and add_sup_callback/1
%% =============================================================================



init([pubsub]) ->
    {ok, #state{}};

init([Fn]) when is_function(Fn, 1) ->
    {ok, #state{callback = Fn}}.


handle_event({Event, Message}, #state{callback = undefined} = State) ->
    %% This is the pubsub handler instance
    %% We notify gproc conditional subscribers
    _ = gproc_ps:publish_cond(l, Event, Message),
    {ok, State};

handle_event({Event, Message}, State) ->
    %% We notify callback funs
    (State#state.callback)({Event, Message}),
    {ok, State}.


handle_call(_Request, State) ->
    {ok, ok, State}.


handle_info(_Info, State) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.