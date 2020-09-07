%% -----------------------------------------------------------------------------
%% @doc Reliable is an OTP application that offers a solution to the problem of
%% ensuring a sequence of Riak KV operations are guaranteed to occur, and to
%% occur in order.
%% The problem arises when one wants to write multiple associated objects to
%% Riak KV which does not support multi-key atomicity, including but not
%% exclusively, the update of application maintained secondary indices after a
%% write.
%% @end
%% -----------------------------------------------------------------------------
-module(reliable).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-type work_id()         ::  reliable_storage_backend:work_id().
-type work_item()       ::  [{
    reliable_storage_backend:work_item_id(),
    reliable_storage_backend:work_item()
}].
-type opts()            ::  #{
    partition_key => binary()
}.

-export_type([work_id/0]).
-export_type([work_item/0]).
-export_type([opts/0]).

-export([enqueue/2]).
-export([enqueue/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(WorkId :: work_id(), WorkItems :: [work_item()]) -> ok.

enqueue(WorkId, WorkItems) ->
    enqueue(WorkId, WorkItems, #{}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(
    WorkId :: work_id(), WorkItems :: [work_item()], Opts :: opts()) ->
    ok.

enqueue(WorkId, WorkItems0, Opts) ->
    %% Add result field expected by reliable_storage_backend:work_item().
    WorkItems = lists:map(
        fun({Id, MFA}) -> {Id, MFA, undefined} end,
        WorkItems0
    ),
    PartitionKey = maps:get(partition_key, Opts, undefined),
    reliable_storage_backend:enqueue({WorkId, WorkItems}, PartitionKey).

