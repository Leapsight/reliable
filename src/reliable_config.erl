
%% =============================================================================
%%  reliable_sup.erl -
%%
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

-module(reliable_config).
-behaviour(app_config).

-define(APP, reliable).

-define(DEFAULT_BACKEND, reliable_riak_store_backend).
-define(WORKER_DEFAULT_RETRY, #{
    backoff_type => jitter,
    backoff_min => timer:seconds(1),
    backoff_max => timer:minutes(1),
    deadline => timer:minutes(15),
    max_retries => 50
}).

%% API
-export([init/0]).
-export([get/1]).
-export([get/2]).
-export([set/2]).
-export([will_set/2]).
-export([on_set/2]).


%% API
-export([partition/1]).
-export([partition_map/0]).
-export([all_partitions/0]).
-export([local_partitions/0]).
-export([number_of_partitions/0]).
-export([instances/0]).
-export([instance_name/0]).
-export([riak_host/0]).
-export([riak_port/0]).
-export([storage_backend/0]).


-eqwalizer({nowarn_function, storage_backend/0}).
-eqwalizer({nowarn_function, riak_host/0}).
-eqwalizer({nowarn_function, riak_port/0}).
-eqwalizer({nowarn_function, instance_name/0}).
-eqwalizer({nowarn_function, instances/0}).
-eqwalizer({nowarn_function, number_of_partitions/0}).
-eqwalizer({nowarn_function, partition_map/0}).
-eqwalizer({nowarn_function, local_partitions/0}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init() ->
    ok = application:set_env(riakc, allow_listing, true),
    ok = app_config:init(?APP, #{callback_mod => ?MODULE}),
    validate().


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec will_set(Key :: key_value:key(), Value :: any()) ->
    ok | {ok, NewValue :: any()} | {error, Reason :: any()}.

will_set(worker_backoff, Value) when is_list(Value) ->
    {ok, maps:from_list(Value)};

will_set(worker_backoff, Value) when is_map(Value) ->
    ok;

will_set(worker_backoff, Value) ->
    {error, {badarg, [worker_backoff, Value]}};

will_set(_, _) ->
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec on_set(Key :: key_value:key(), Value :: any()) -> ok.

on_set(_, _) ->
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(Key :: list() | atom() | tuple()) -> term().

get(worker_retry) ->
    get(worker_retry, ?WORKER_DEFAULT_RETRY);

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
%% @doc The instance that is managed by this node
%% @end
%% -----------------------------------------------------------------------------
-spec instance_name() -> binary() | no_return().

instance_name() ->
    app_config:get(?APP, instance_name).


%% -----------------------------------------------------------------------------
%% @doc The number of partitions per instance
%% @end
%% -----------------------------------------------------------------------------
-spec number_of_partitions() -> integer().

number_of_partitions() ->
    app_config:get(?APP, number_of_partitions, 3).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec all_partitions() -> [binary()].

all_partitions() ->
    lists:flatten(maps:values(partition_map())).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partition_map() -> #{binary() => [binary()]}.

partition_map() ->
    case app_config:get(?APP, partition_map, undefined) of
        undefined ->
            Map = gen_partitions(),
            ok = set(partition_map, Map),
            Map;
        Map  when is_map(Map)->
            Map
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec local_partitions() -> [binary()].

local_partitions() ->
    case ?MODULE:get(instance_name, undefined) of
        undefined ->
            partition_map();
        Name ->
            maps:get(Name, partition_map())
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partition(Key :: binary() | undefined) -> binary().

partition(undefined) ->
    do_partition(rand:uniform(maps:size(partition_map())));

partition(Key) ->
    do_partition(erlang:phash2(Key)).




%% =============================================================================
%% PRIVATE
%% =============================================================================



validate() ->
    ok = validate([
        ?MODULE:get(instance_name, {error, instance_name})
    ]),
    %% We force the generation of properties
    _  = partition_map(),
    ok.



validate([{error, Key}|_]) ->
    error({invalid_config, Key});

validate([_|T]) ->
    validate(T);

validate([]) ->
    ok.


%% @private
do_partition(Hash) when is_integer(Hash) ->
    Partitions = partition_map(),

    %% We choose the instance and its partitions. We do this to ensure changes
    %% to the number_of_partitions will not change the instance associated for
    %% Key.
    Instances = maps:keys(Partitions),
    M = Hash rem length(Instances) + 1,
    MPartitions = maps:get(lists:nth(M, Instances), Partitions),

    %% We choose a partition
    N = Hash rem number_of_partitions() + 1,
    lists:nth(N, MPartitions).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% If instances are ["foo-0", "foo-1"] and N is 3 this will generate
%% ```
%% #{
%%  <<"foo-0">> => [
%%         <<"foo-0_partition_1">>,
%%         <<"foo-0_partition_2">>,
%%         <<"foo-0_partition_3">>
%%    ]
%%  <<"foo-1" => [
%%         <<"foo-1_partition_1">>,
%%         <<"foo-1_partition_2">>,
%%         <<"foo-1_partition_3">>
%%    ]
%% }.
%% '''
%% @end
%% -----------------------------------------------------------------------------
-spec gen_partitions() -> #{binary() => [binary()]}.

gen_partitions() ->
    Instances = instances(),
    N = number_of_partitions(),

    %% We validate
    lists:member(instance_name(), instances())
    orelse error({
        invalid_config,
        "The value for instance_name is in the list of instances."
    }),

    lists:foldl(
        fun(Instance, Acc) ->
            Partitions = [
                <<Instance/binary, "_partition_", (integer_to_binary(X))/binary>>
                || X <- lists:seq(1, N)
            ],
            maps:put(Instance, Partitions, Acc)
        end,
        maps:new(),
        Instances
    ).