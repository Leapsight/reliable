-module(reliable_riak_storage_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-include_lib("kernel/include/logger.hrl").

-export([init/0,
         enqueue/2,
         enqueue/3,
         delete_all/2,
         update/3,
         fold/3]).

init() ->
    Host = application:get_env(reliable, riak_host, "127.0.0.1"),
    Port = application:get_env(reliable, riak_port, 80087),
    case riakc_pb_socket:start_link(Host, Port) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

enqueue(Reference, {WorkId, WorkItems}) ->
    Object = riakc_obj:new(bucket(), term_to_binary(WorkId), term_to_binary(WorkItems)),
    riakc_pb_socket:put(Reference, Object).

enqueue(Reference, PartitionKey, {WorkId, WorkItems}) ->
    Object = riakc_obj:new(bucket(PartitionKey), term_to_binary(WorkId), term_to_binary(WorkItems)),
    riakc_pb_socket:put(Reference, Object).

delete_all(Reference, WorkIds) ->
    lists:foreach(fun(Key) -> riakc_pb_socket:delete(Reference, bucket(), Key) end, WorkIds),
    ok.

update(Reference, WorkId, WorkItems) ->
    case riakc_pb_socket:get(Reference, bucket(), WorkId) of
        {ok, O} ->
            O1 = riakc_obj:update_value(O, WorkItems),
            case riakc_pb_socket:put(Reference, O1, [return_body]) of
                {ok, _O2} ->
                    ok;
                {error, Reason} ->
                    ?LOG_ERROR(
                        "~p: failed to update object: ~p", [?MODULE, Reason]
                    ),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?LOG_ERROR(
                "~p: failed to read object before update: ~p",
                [?MODULE, Reason]
            ),
            {error, Reason}
    end.

fold(Reference, Function, Acc) ->
    %% Get list of the keys in the bucket.
    {ok, Keys} = riakc_pb_socket:list_keys(Reference, bucket()),
    ?LOG_DEBUG("~p: got work keys: ~p", [?MODULE, Keys]),

    %% Fold the keys.
    FoldFun = fun(Key, Acc1) ->
        %% Get the keys value.
        case riakc_pb_socket:get(Reference, bucket(), Key) of
            {ok, Object} ->
                BinaryData = riakc_obj:get_value(Object),
                TermData = binary_to_term(BinaryData),
                ?LOG_DEBUG("~p: got key: ~p", [?MODULE, Key]),
                ?LOG_DEBUG("~p: got term data: ~p", [?MODULE, TermData]),
                Function({Key, TermData}, Acc1);
            {error, Reason} ->
                ?LOG_ERROR(
                    "~p: can't handle response from pb socket: ~p",
                    [?MODULE, Reason]
                ),
                Acc1
        end
    end,
    lists:foldl(FoldFun, Acc, Keys).

%% @private
bucket(WorkId) ->
    AllInstances = application:get_env(reliable, instances, ["default_instance"]),
    HashedWorkId = hash(WorkId),
    InstanceId = HashedWorkId rem length(AllInstances) + 1,
    InstanceName = lists:nth(InstanceId, AllInstances),
    BucketNameList = "work" ++ "_" ++ InstanceName,
    BucketName = list_to_binary(BucketNameList),
    BucketName.

bucket() ->
    InstanceName = application:get_env(reliable, instance_name, "default_instance"),
    BucketNameList = "work" ++ "_" ++ InstanceName,
    BucketName = list_to_binary(BucketNameList),
    BucketName.

%% @private
hash(Key) ->
    erlang:phash2(Key).