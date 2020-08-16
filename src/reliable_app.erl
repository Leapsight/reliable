-module(reliable_app).

-behaviour(application).

-export([start/2,
         stop/1]).

start(_StartType, _StartArgs) ->
    ok = application:set_env(riakc, allow_listing, true),
    reliable_sup:start_link().

stop(_State) ->
    ok.