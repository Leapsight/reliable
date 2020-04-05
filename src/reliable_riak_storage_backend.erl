-module(reliable_riak_storage_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-define(BUCKET, <<"work">>).

-export([init/0,
         enqueue/2,
         delete_all/2,
         update/3,
         fold/3]).

init() ->
    case riakc_pb_socket:start_link("127.0.0.1", 8087) of 
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

enqueue(Reference, {WorkId, WorkItems}) ->
    Object = riakc_obj:new(?BUCKET, term_to_binary(WorkId), term_to_binary(WorkItems)),
    riakc_pb_socket:put(Reference, Object).

delete_all(Reference, WorkIds) ->
    lists:foreach(fun(Key) -> riakc_pb_socket:delete(Reference, ?BUCKET, Key) end, WorkIds),
    ok.

update(Reference, WorkId, WorkItems) ->
    case riakc_pb_socket:get(Reference, ?BUCKET, WorkId) of 
        {ok, O} ->
            O1 = riakc_obj:update_value(O, WorkItems),
            case riakc_pb_socket:put(Reference, O1, [return_body]) of 
                {ok, _O2} ->
                    ok;
                {error, Reason} ->
                    error_logger:format("~p: failed to update object: ~p", [?MODULE, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            error_logger:format("~p: failed to read object before update: ~p", [?MODULE, Reason]),
            {error, Reason}
    end.

fold(Reference, Function, Acc) ->
    %% Get list of the keys in the bucket.
    Keys = riakc_pb_socket:list_keys(Reference, ?BUCKET),

    %% Fold the keys.
    FoldFun = fun(Key, Acc1) ->
        %% Get the keys value.
        case riakc_pb_socket:get(Reference, ?BUCKET, Key) of 
            {ok, Object} ->
                BinaryData = riakc_obj:get_value(Object),
                TermData = binary_to_term(BinaryData),
                Function({Key, TermData}, Acc1);
            {error, Reason} ->
                error_logger:format("~p: can't handle response from pb socket: ~p", [?MODULE, Reason]),
                Acc1
        end
    end,
    lists:foldl(FoldFun, Acc, Keys).