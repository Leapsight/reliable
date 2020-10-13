-module(reliable_sup).
-behaviour(supervisor).

-define(SUPERVISOR(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => supervisor,
    modules => [Id]
}).

-define(WORKER(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [Id]
}).

-define(EVENT_MANAGER(Id, Restart, Timeout), #{
    id => Id,
    start => {gen_event, start_link, [{local, Id}]},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [dynamic]
}).


%% API
-export([start_link/0]).
-export([init/1]).



%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    try reliable_config:init() of
        ok ->
        supervisor:start_link({local, ?MODULE}, ?MODULE, [])
    catch
        error:Reason ->
            {error, Reason}
    end.



%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================



init([]) ->

    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },

    ChildSpecs = [
        ?SUPERVISOR(
            reliable_event_handler_watcher_sup, [], permanent, infinity
        ),
        ?EVENT_MANAGER(reliable_event_manager, permanent, 5000),
        ?SUPERVISOR(reliable_worker_sup, [], permanent, infinity)
    ],

    {ok, {SupFlags, ChildSpecs}}.




