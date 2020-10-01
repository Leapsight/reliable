-module(reliable_config).

-define(DEFAULT_BACKEND, reliable_riak_storage_backend).

%% API
-export([partition/1]).
-export([partitions/0]).
-export([number_of_partitions/0]).
-export([instance_name/0]).
-export([riak_host/0]).
-export([riak_port/0]).
-export([setup/0]).
-export([storage_backend/0]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec setup() -> ok.

setup() ->
    ok = application:set_env(riakc, allow_listing, true),
    ok.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec storage_backend() -> module().

storage_backend() ->
    application:get_env(reliable, backend, ?DEFAULT_BACKEND).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec riak_host() -> list().

riak_host() ->
    application:get_env(reliable, riak_host, "127.0.0.1").


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec riak_port() -> integer().

riak_port() ->
    application:get_env(reliable, riak_port, 8087).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec instance_name() -> [binary()].

instance_name() ->
    application:get_env(reliable, instance_name, <<"default">>).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec number_of_partitions() -> [binary()].

number_of_partitions() ->
    application:get_env(reliable, number_of_partitions, 5).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partitions() -> [binary()].

partitions() ->
    Name = instance_name(),
    [
        <<Name/binary, "_work_queue_", (integer_to_binary(X))/binary>>
        || X <- lists:seq(1, number_of_partitions())
    ].


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


