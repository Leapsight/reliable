-module(reliable_SUITE).

-export([all/0]).

-export([basic_test/1]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [basic_test].

basic_test(_Config) ->

    %% Start the application.
    application:ensure_all_started(reliable),

    %% Enqueue a write into Riak.
    Object = riakc_obj:new(<<"groceries">>, <<"mine">>, <<"eggs & bacon">>),

    Node = node(),
    Module = riakc_pb_socket,
    Function = put,
    Args = [{symbolic, riakc}, Object],
    WorkItems = [{1, {Node, Module, Function, Args}, undefined}],

    WorkId = basic, 
    Work = {WorkId, WorkItems},
    ok = reliable_storage_backend:enqueue(Work),

    %% Sleep for 5 seconds.
    timer:sleep(5000),

    %% Attempt to read the object back.
    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    error_logger:format("~p: got connection to Riak: ~p", [?MODULE, RiakcPid]),
    pong = riakc_pb_socket:ping(RiakcPid),

    {ok, O} = riakc_pb_socket:get(RiakcPid, <<"groceries">>, <<"mine">>),
    <<"eggs & bacon">> = riakc_obj:get_value(O),

    %% Terminate the application.
    application:stop(reliable),

    ok.