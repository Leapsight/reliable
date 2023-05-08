-module(reliable_queue).

-record(reliable_queue, {
    store           ::  dets:tab_name(),
    filename        ::  file:name(),
    pending_acks    ::  #{pos_integer() => binary()}


}).


-type t()           :: #reliable_queue{}.

-export_type([t/0]).

-export([ack/2]).
-export([dequeue/1]).
-export([enqueue/2]).
-export([in_flight/1]).
-export([new/2]).
-export([peek/1]).



%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new(Name :: binary(), Opts :: map()) -> t() | no_return().

new(Name, Opts) ->
    _File = filename(maps:get(data_dir, Opts, ".data/"), Name),

    error(not_implemented).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(Item :: any(), Queue :: t()) -> ok.

enqueue(_Item, _Queue) ->
    error(not_implemented).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec dequeue(Queue :: t()) -> ok.

dequeue(_Queue) ->
    error(not_implemented).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec peek(Queue :: t()) -> ok.

peek(_Queue) ->
    error(not_implemented).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec ack(Id :: any(), Queue :: t()) -> ok.

ack(_Id, _Queue) ->
    error(not_implemented).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec in_flight(Queue :: t()) -> ok.

in_flight(_Queue) ->
    error(not_implemented).




%% =============================================================================
%% PRIVATE
%% =============================================================================


filename(Path, Name) when is_binary(Path) ->
    filename(binary_to_list(Path), Name);

filename(Path, Name) when is_binary(Name) ->
    filename(Path, binary_to_list(Name));

filename(Path, Name) when is_list(Path), is_list(Name) ->
    filename:join(Path, Name).


