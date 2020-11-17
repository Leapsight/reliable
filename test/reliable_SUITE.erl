-module(reliable_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").


-compile([nowarn_export_all, export_all]).


all() ->
    [
        basic_test,
        index_test,
        workflow_do_nothing_test,
        workflow_error_test,
        workflow_1_test
    ].

init_per_suite(Config) ->
    %% Allow keylisting.
    application:set_env(riakc, allow_listing, true),
    application:ensure_all_started(reliable),
    logger:set_application_level(reliable, debug),
    meck:unload(),

    %% meck:new(reliable, [passthrough]),
    %% We remove all work from the queues
    ok = reliable_partition_store:flush_all(),
    ct:pal("Env ~p~n", [application:get_all_env(reliable)]),
    Config.

end_per_suite(Config) ->
    %% meck:unload(),
    %% Terminate the application.
    application:stop(reliable),
    {save_config, Config}.


init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),
    _Config.


end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),
    _Config.


basic_test(_Config) ->
    %% Enqueue a write into Riak.
    Object = riakc_obj:new(<<"groceries">>, <<"mine">>, <<"eggs & bacon">>),
    Work = [
        {1,
            reliable_task:new(
                node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]
            )
        }
    ],
    {ok, WorkRef} = reliable:enqueue(Work, #{work_id => <<"basic">>}),

    %% Sleep for 5 seconds for write to happen.
    {in_progress, _} = reliable:status(WorkRef),
    {ok, _} = reliable:yield(WorkRef, 5000),

    %% Attempt to read the object back.
    {ok, Conn} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(Conn),
    {ok, O} = riakc_pb_socket:get(Conn, <<"groceries">>, <<"mine">>),
    <<"eggs & bacon">> = riakc_obj:get_value(O),

    ok.


index_test(_Config) ->
    %% Enqueue a write of object into Riak along with an index update.
    Object = riakc_obj:new(<<"users">>, <<"cmeik">>, <<"something">>),
    Index = riakc_set:new(),
    Index1 = riakc_set:add_element(<<"cmeik">>, Index),
    Work = [
        {1,
            reliable_task:new(
                node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]
            )
        },
        {2,
            reliable_task:new(
                node(), riakc_pb_socket, update_type,
                [{symbolic, riakc}, {<<"sets">>, <<"users">>}, <<"users">>, riakc_set:to_op(Index1)]
            )
        }
    ],

    {ok, WorkRef} = reliable:enqueue(Work, #{work_id => <<"cmeik">>}),

    {in_progress, _} = reliable:status(WorkRef),
    {ok, _} = reliable:yield(WorkRef, 5000),

    %% Attempt to read the user object back.
    {ok, Conn} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(Conn),
    {ok, O} = riakc_pb_socket:get(Conn, <<"users">>, <<"cmeik">>),
    <<"something">> = riakc_obj:get_value(O),
    {ok, I} = riakc_pb_socket:fetch_type(Conn, {<<"sets">>, <<"users">>}, <<"users">>),
    ?assertEqual(true, lists:member(<<"cmeik">>, riakc_set:value(I))),

    ok.


workflow_do_nothing_test(_) ->
    {false, #{result := foo}} = reliable:workflow(fun() -> foo end).


workflow_error_test(_) ->
    ?assertEqual({error, foo}, reliable:workflow(fun() -> throw(foo) end)),
    ?assertError(foo, reliable:workflow(fun() -> error(foo) end)),
    ?assertError(foo, reliable:workflow(fun() -> exit(foo) end)).


workflow_1_test(_) ->

    Object = riakc_obj:new(<<"users">>, <<"aramallo">>, <<"something_else">>),
    Index = riakc_set:new(),
    Index1 = riakc_set:add_element(<<"aramallo">>, Index),

    %% We defined them outside the workflow fun just to use the vars in the mock
    A = {update,
        reliable_task:new(
            node(), riakc_pb_socket, put, [{symbolic, riakc}, Object]
        )
    },
    B = {update,
            reliable_task:new(
                node(), riakc_pb_socket, update_type,
                [
                    {symbolic, riakc},
                    {<<"sets">>, <<"users">>},
                    <<"users">>,
                    riakc_set:to_op(Index1)
                ]
            )
    },

    Fun = fun() ->
        ok = reliable:add_workflow_items([{a, A}]),
        ok = reliable:add_workflow_items([{b, B}]),
        ok = reliable:add_workflow_precedence(a, b),
        ok = reliable:set_workflow_event_payload(#{items => [a, b]}),
        ok
    end,

    {true, Result} = reliable:workflow(Fun, #{subscribe => true}),
    #{result := ok, work_ref := WorkRef} = Result,
    %% {ok, Event} = reliable:yield(WorkRef, 5000),
    %% ?assertEqual(completed, maps:get(status, Event)),
    %% ?assertEqual(WorkRef, maps:get(work_ref, Event)),
    {ok, _} = reliable:yield(WorkRef, 5000),

    %% Attempt to read the user object back.
    {ok, Conn} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    pong = riakc_pb_socket:ping(Conn),
    {ok, O} = riakc_pb_socket:get(Conn, <<"users">>, <<"aramallo">>),
    <<"something_else">> = riakc_obj:get_value(O),
    {ok, I} = riakc_pb_socket:fetch_type(
        Conn, {<<"sets">>, <<"users">>}, <<"users">>),
    ?assertEqual(true, lists:member(<<"aramallo">>, riakc_set:value(I))),
    ok.

