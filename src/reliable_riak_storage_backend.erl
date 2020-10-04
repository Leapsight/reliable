-module(reliable_riak_storage_backend).
-behaviour(reliable_storage_backend).

-include_lib("kernel/include/logger.hrl").
-include_lib("riakc/include/riakc.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([init/0,
         enqueue/3,
         get/2,
         delete/3,
         delete_all/3,
         update/4,
         fold/5]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
init() ->
    Host = application:get_env(reliable, riak_host, "127.0.0.1"),
    Port = application:get_env(reliable, riak_port, 8087),

    case riakc_pb_socket:start_link(Host, Port) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
enqueue(Reference, Bucket, {WorkId, WorkItems}) ->
    Object = riakc_obj:new(
        Bucket,
        WorkId,
        term_to_binary(WorkItems)
    ),
    riakc_pb_socket:put(Reference, Object).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get(Reference, {work_ref, Bucket, WorkId}) ->
    case riakc_pb_socket:get(Reference, Bucket, WorkId, []) of
        {ok, Object} ->
            BinaryData = riakc_obj:get_value(Object),
            TermData = binary_to_term(BinaryData),
            {ok, TermData};
        {error, notfound} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.





%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
delete(Reference, Bucket, WorkId) ->
    riakc_pb_socket:delete(Reference, Bucket, WorkId).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
delete_all(Reference, Bucket, WorkIds) ->
    _ = lists:foreach(
        fun(WorkId) -> delete(Reference, Bucket, WorkId) end,
        WorkIds
    ),
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
update(Reference, Bucket, WorkId, WorkItems) ->
    case riakc_pb_socket:get(Reference, Bucket, WorkId) of
        {ok, O} ->
            O1 = riakc_obj:update_value(O, WorkItems),
            case riakc_pb_socket:put(Reference, O1, [return_body]) of
                {ok, _O2} ->
                    ok;
                {error, Reason} ->
                    ?LOG_ERROR("failed to update object: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?LOG_ERROR(
                "~p: failed to read object before update: ~p",
                [?MODULE, Reason]
            ),
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------

fold(Reference, Bucket, Function, Acc, Opts) ->
    ReqOpts = fold_opts(Opts),
    %% Get list of the keys in the bucket.
    %% We use the $bucket secondary index so that we can do pagination with
    %% sorting.
    Res = riakc_pb_socket:get_index_eq(
        Reference,
        Bucket,
        <<"$bucket">>,
        <<>>,
        ReqOpts
    ),

    #index_results_v1{keys = Keys, continuation = Cont1} = maybe_error(Res),

    ?LOG_DEBUG("Got work keys: ~p", [Keys]),

    FoldFun = fun(Key, Acc1) ->
        case riakc_pb_socket:get(Reference, Bucket, Key) of
            {ok, Object} ->
                BinaryData = riakc_obj:get_value(Object),
                TermData = binary_to_term(BinaryData),
                ?LOG_DEBUG("got key: ~p", [Key]),
                ?LOG_DEBUG("got term data: ~p", [TermData]),
                Function({Key, TermData}, Acc1);
            {error, Reason} ->
                ?LOG_ERROR(
                    "Can't handle response from pb socket; reason=~p",
                    [Reason]
                ),
                Acc1
        end
    end,
    Result = lists:foldl(FoldFun, Acc, Keys),
    {Result, Cont1}.




%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
fold_opts(Opts0) ->
    Default = #{
        max_results => 10,
        pagination_sort => true,
        timeout => 30000
    },
    Opts1 = maps:merge(Default, Opts0),
    case maps:get(continuation, Opts1, undefined) of
        undefined ->
            maps:to_list(Opts1);
        Cont0 ->
            maps:to_list(Opts1#{continuation => Cont0})
    end.



maybe_error({ok, Result}) -> Result;
maybe_error({error, "overload"}) -> error(overload);
maybe_error({error, Reason}) -> error(Reason).