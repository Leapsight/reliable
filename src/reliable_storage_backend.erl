-module(reliable_storage_backend).


-callback init() -> {ok, pid()} | {error, any()}.


-callback enqueue(
    Ref :: pid(), Bucket :: binary(), Work :: reliable_worker:work()) ->
    ok | {error, any()}.


-callback update(
    Ref :: pid(),
    Bucket :: binary(),
    WorkId :: reliable_worker:work_id(),
    NewItems :: [reliable_worker:work_item()]) -> ok | {error, any()}.


-callback fold(
    Ref :: pid(), Bucket :: binary(), Fun :: function(), Acc :: any()) ->
    NewAcc :: any().


-callback delete(
    Ref :: pid(),
    Bucket :: binary(),
    WorkId :: reliable_worker:work_id()) -> ok.


-callback delete_all(
    Ref :: pid(),
    Bucket :: binary(),
    AllCompleted :: [reliable_worker:work_id()]) -> ok.