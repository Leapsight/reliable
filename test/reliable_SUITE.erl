-module(reliable_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([basic_test/1,
         index_test/1,
         workflow_do_nothing_test/1,
         workflow_error_test/1,
         workflow_test/1]).


all() ->
    [
        basic_test,
        index_test,
        workflow_do_nothing_test,
        workflow_error_test,
        workflow_test
    ].

init_per_suite(Config) ->
    meck:unload(),
    Config.

end_per_suite(Config) ->
    meck:unload(),
    {save_config, Config}.


init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    %% Allow keylisting.
    application:set_env(riakc, allow_listing, true),

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
    Work = [{1, {node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]}}],
    {ok, _} = reliable:enqueue(<<"basic">>, Work),

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

    Work = [
        {1, {node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]}},
        {2, {node(), riakc_pb_socket, update_type,
            [{symbolic, riakc}, {<<"sets">>, <<"users">>}, <<"users">>, riakc_set:to_op(Index1)]}}
    ],
    {ok, _} = reliable:enqueue(<<"cmeik">>, Work),

    %% Sleep for 5 seconds for write to happen.
    timer:sleep(5000),

    %% Attempt to read the user object back.
    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(RiakcPid),
    {ok, O} = riakc_pb_socket:get(RiakcPid, <<"users">>, <<"cmeik">>),
    <<"something">> = riakc_obj:get_value(O),
    {ok, I} = riakc_pb_socket:fetch_type(RiakcPid, {<<"sets">>, <<"users">>}, <<"users">>),
    ?assertEqual(true, lists:member(<<"cmeik">>, riakc_set:value(I))),

    ok.


workflow_do_nothing_test(_) ->
    {ok, foo} = reliable:workflow(fun() -> foo end).


workflow_error_test(_) ->
    ?assertEqual({error, foo}, reliable:workflow(fun() -> throw(foo) end)),
    ?assertError(foo, reliable:workflow(fun() -> error(foo) end)),
    ?assertError(foo, reliable:workflow(fun() -> exit(foo) end)).


workflow_test(_) ->
    Object = riakc_obj:new(<<"users">>, <<"aramallo">>, <<"something_else">>),
    Index = riakc_set:new(),
    Index1 = riakc_set:add_element(<<"aramallo">>, Index),

    %% We defined them outside the workflow fun just to use the vars in the mock
    A = {update, {node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]}},
    B = {update,
            {node(), riakc_pb_socket, update_type,[
                {symbolic, riakc},
                {<<"sets">>, <<"users">>},
                <<"users">>,
                riakc_set:to_op(Index1)
            ]}
        },

    Fun = fun() ->
        ok = reliable:add_workflow_items([{a, A}]),
        ok = reliable:add_workflow_items([{b, B}]),
        ok = reliable:add_workflow_precedence(a, b),
        ok
    end,

    %% Not really storing the index, we intercept the reliable enqueue call
    %% here to validate we are getting the right struct
    meck:new(reliable, [passthrough]),
    true = meck:validate(reliable),

    meck:expect(reliable, enqueue, fun
        (_, Work) ->
            ?assertEqual([{1, A}, {2, B}], Work),
            ok
    end),

    {scheduled, _, ok} = reliable:workflow(Fun),

    %% Sleep for 5 seconds for write to happen.
    timer:sleep(5000),

    %% Attempt to read the user object back.
    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(RiakcPid),
    {ok, O} = riakc_pb_socket:get(RiakcPid, <<"users">>, <<"aramallo">>),
    <<"something_else">> = riakc_obj:get_value(O),
    {ok, I} = riakc_pb_socket:fetch_type(RiakcPid, {<<"sets">>, <<"users">>}, <<"users">>),
    ?assertEqual(true, lists:member(<<"aramallo">>, riakc_set:value(I))),
    ok.