-module(reliable_SUITE).

-export([all/0,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([basic_test/1,
         index_test/1]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [basic_test, 
     index_test].

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    %% Start the application.
    application:ensure_all_started(reliable),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    %% Terminate the application.
    application:stop(reliable),

    _Config.

basic_test(_Config) ->
    %% Enqueue a write into Riak.
    Object = riakc_obj:new(<<"groceries">>, <<"mine">>, <<"eggs & bacon">>),
    Work = {basic, [{1, {node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]}, undefined}]},
    ok = reliable_storage_backend:enqueue(Work),

    %% Sleep for 5 seconds for write to happen.
    timer:sleep(5000),

    %% Attempt to read the object back.
    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(RiakcPid),
    {ok, O} = riakc_pb_socket:get(RiakcPid, <<"groceries">>, <<"mine">>),
    <<"eggs & bacon">> = riakc_obj:get_value(O),

    ok.

index_test(_Config) ->
    %% Enqueue a write of object into Riak along with an index update.
    Object = riakc_obj:new(<<"users">>, <<"cmeik">>, <<"something">>),
    Index = riakc_set:new(),
    Index1 = riakc_set:add_element(<<"cmeik">>, Index),

    Work = {cmeik, [
                    {1, {node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]}, undefined},
                    {2, {node(), riakc_pb_socket, update_type, 
                        [{symbolic, riakc}, {<<"sets">>, <<"users">>}, <<"users">>, riakc_set:to_op(Index1)]}, undefined}
                   ]},
    ok = reliable_storage_backend:enqueue(Work),

    %% Sleep for 5 seconds for write to happen.
    timer:sleep(5000),

    %% Attempt to read the user object back.
    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(RiakcPid),
    {ok, O} = riakc_pb_socket:get(RiakcPid, <<"users">>, <<"cmeik">>),
    <<"something">> = riakc_obj:get_value(O),
    {ok, I} = riakc_pb_socket:fetch_type(RiakcPid, {<<"sets">>, <<"users">>}, <<"users">>),
    [<<"cmeik">>] = riakc_set:value(I),

    ok.