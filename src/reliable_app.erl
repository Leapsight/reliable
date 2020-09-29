-module(reliable_app).

-behaviour(application).

-export([start/2,
         stop/1]).




%% =============================================================================
%% APPLICATION CALLBACKS
%% =============================================================================



start(_StartType, _StartArgs) ->
    ok = reliable_config:setup(),
    case reliable_sup:start_link() of
        {ok, _} = OK -> OK;
        {error, _} = Error -> Error
    end.

stop(_State) ->
    ok.
