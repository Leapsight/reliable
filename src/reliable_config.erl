-module(reliable_config).
-behaviour(app_config).

-define(APP, reliable).

-define(DEFAULT_BACKEND, reliable_riak_storage_backend).

%% APP_CONFIG CALLBACKS
-export([on_set/2]).
-export([will_set/2]).

%% API
-export([init/0]).
-export([get/1]).
-export([get/2]).
-export([set/2]).


%% API
-export([partition/1]).
-export([partitions/0]).
-export([number_of_partitions/0]).
-export([instances/0]).
-export([instance_name/0]).
-export([riak_host/0]).
-export([riak_port/0]).
-export([storage_backend/0]).






%% =============================================================================
%% APP_CONFIG CALLBACKS
%% =============================================================================




-spec on_set(Key :: key_value:key(), Value :: any()) -> ok.

on_set(number_of_partitions, Value) when is_integer(Value) ->
    Partitions = gen_partitions(Value),
    set(Partitions, Partitions);

on_set(_, _) ->
    ok.


-spec will_set(Key :: key_value:key(), Value :: any()) ->
    ok | {ok, NewValue :: any()} | {error, Reason :: any()}.

will_set(_Key, Value) ->
    {ok, Value}.




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init() ->
    ok = application:set_env(riakc, allow_listing, true),
    app_config:init(?APP, #{callback_mod => ?MODULE}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(Key :: list() | atom() | tuple()) -> term().

get(Key) ->
    app_config:get(?APP, Key).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(Key :: list() | atom() | tuple(), Default :: term()) -> term().

get(Key, Default) ->
    app_config:get(?APP, Key, Default).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set(Key :: key_value:key() | tuple(), Value :: term()) -> ok.

set(Key, Value) ->
    app_config:set(?APP, Key, Value).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec storage_backend() -> module().

storage_backend() ->
    app_config:get(?APP, backend, ?DEFAULT_BACKEND).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec riak_host() -> list().

riak_host() ->
    app_config:get(?APP, riak_host, "127.0.0.1").


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec riak_port() -> integer().

riak_port() ->
    app_config:get(?APP, riak_port, 8087).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec instances() -> [binary()].

instances() ->
    case app_config:get(?APP, instances, undefined) of
        undefined -> [instance_name()];
        List -> List
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec instance_name() -> [binary()].

instance_name() ->
    app_config:get(?APP, instance_name, <<"default">>).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec number_of_partitions() -> [binary()].

number_of_partitions() ->
    app_config:get(?APP, number_of_partitions, 5).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partitions() -> [binary()].

partitions() ->
    %% They are set in on_set/2
    case app_config:get(?APP, partitions, undefined) of
        undefined ->
            Partitions = gen_partitions(number_of_partitions()),
            app_config:set(?APP, partitions, Partitions),
            Partitions;
        Partitions ->
            Partitions
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partition(Key :: binary() | undefined) -> binary().

partition(undefined) ->
    lists:nth(rand:uniform(number_of_partitions()), partitions());

partition(Key) ->
    N = erlang:phash2(Key) rem number_of_partitions() + 1,
    lists:nth(N, partitions()).





%% =============================================================================
%% PRIVATE
%% =============================================================================



gen_partitions(N) ->
    Name = instance_name(),
    [
        <<Name/binary, "_work_queue_", (integer_to_binary(X))/binary>>
        || X <- lists:seq(1, N)
    ].