-module(reliable_dets_storage_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-define(TABLE, reliable_backend).
-define(FILENAME, "reliable-backend-data").

-export([init/0,
         enqueue/2,
         delete_all/2,
         update/3,
         fold/3]).

init() ->
    case dets:open_file(?TABLE, [{file, ?FILENAME}]) of 
        {ok, Reference} ->
            {ok, Reference};
        {error, Reason} ->
            {error, Reason}
    end.

enqueue(Reference, Work) ->
    case dets:insert_new(Reference, Work) of 
        true ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

delete_all(Reference, WorkIds) ->
    lists:foreach(fun(Key) -> dets:delete(Reference, Key) end, WorkIds),
    ok.

update(Reference, WorkId, WorkItems) ->
    dets:insert(Reference, {WorkId, WorkItems}).

fold(Reference, Function, Acc) ->
    dets:foldl(Function, Acc, Reference).