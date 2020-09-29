-module(reliable_worker_sup).
-behaviour(supervisor).


%% API
-export([start_link/0]).
-export([init/1]).



%% =============================================================================
%% API
%% =============================================================================



start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).




%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================





init([]) ->

    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },

    %% We spawn a child per bucket. Each server is the single writer
    %% to that bucket, the bucket acting as a queue.
    ChildSpecs = [
        begin
            Name = binary_to_atom(Bucket, utf8),
            #{
                id => Name,
                start => {
                    reliable_worker,
                    start_link,
                    [Name, Bucket]
                },
                restart => permanent,
                shutdown => infinity,
                type => worker,
                modules => [reliable_worker]
            }
        end || Bucket <- reliable_config:instances()
    ],

    {ok, {SupFlags, ChildSpecs}}.
