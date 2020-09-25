-module(reliable_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

-define(CHILD(I, Type, Timeout),
        {I, {I, start_link, []}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).




%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).



%% =============================================================================
%% SUPERVISOT CALLBACKS
%% =============================================================================



init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},

    ChildSpecs = [?CHILD(reliable_storage_backend, worker)],

    {ok, {SupFlags, ChildSpecs}}.