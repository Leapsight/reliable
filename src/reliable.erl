-module(reliable).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([enqueue/2]).
-export([enqueue/3]).


enqueue(WorkId, WorkItems0) ->
    %% Add result field.
    WorkItems = lists:map(fun({Id, MFA}) -> {Id, MFA, undefined} end, WorkItems0),
    %% Enqueue.
    reliable_storage_backend:enqueue({WorkId, WorkItems}).


enqueue(WorkId, WorkItems0, PartitionKey) ->
    %% Add result field.
    WorkItems = lists:map(fun({Id, MFA}) -> {Id, MFA, undefined} end, WorkItems0),
    %% Enqueue.
    reliable_storage_backend:enqueue({WorkId, WorkItems}, PartitionKey).