-module(reliable_worker).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/2,
         enqueue/2]).

%% gen_server callbacks
-export([init/1,
         handle_continue/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {
    bucket      ::  binary(),
    backend     ::  module(),
    reference   ::  reference() | pid(),
    symbolics   ::  dict:dict()
}).

%% should be some sort of unique term identifier.
-type work_id() :: term().

%% MFA, and a node to actually execute the RPC at.
-type work_item() :: {node(), module(), function(), [term()]}.

%% the result of any work performed.
-type work_item_result() :: term().

%% identifier for the work item.
-type work_item_id() :: integer().

%% the work.
-type work() :: {
    WorkId :: work_id(),
    Payload :: [{work_item_id(), work_item(), work_item_result()}]
}.

-type work_ref()    ::  {
    work_ref,
    Instance :: binary(),
    Id :: binary()
}.

-export_type([work_ref/0]).
-export_type([work_id/0]).
-export_type([work_item_id/0]).
-export_type([work_item_result/0]).
-export_type([work_item/0]).



%% =============================================================================
%% API
%% =============================================================================



start_link(Name, Bucket) ->
    gen_server:start({local, Name}, ?MODULE, [Bucket], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(work(), binary() | undefined) ->
    {ok, work_ref()} | {error, term()}.

enqueue(Work, PartitionKey) ->
    Instance = binary_to_atom(reliable_config:instance(PartitionKey), utf8),
    gen_server:call(Instance, {enqueue, Work}).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([Bucket]) ->
    ?LOG_DEBUG("Initializing."),

    %% Open storage.
    BackendMod = reliable_config:storage_backend(),

    case BackendMod:init() of
        {ok, Reference} ->
            Conn = get_db_connection(),
            %% Initialize symbolic variable dict.
            Symbolics = dict:store(riakc, Conn, dict:new()),

            State = #state{
                bucket = Bucket,
                backend = BackendMod,
                symbolics = Symbolics,
                reference = Reference
            },
            {ok, State, {continue, schedule_work}};

        {error, Reason} ->
            {stop, {error, Reason}}
    end.


handle_continue(schedule_work, State) ->
    ok = schedule_work(),
    {noreply, State}.


handle_call(
    {enqueue, {WorkId, _} = Work}, _From, #state{bucket = Bucket} = State) ->
    %% TODO: Deduplicate here.
    %% TODO: Replay once completed.
    ?LOG_INFO("Enqueuing work: ~p, instance: ~p", [Work, Bucket]),

    BackendMod = State#state.backend,
    Ref = State#state.reference,

    case BackendMod:enqueue(Ref, Bucket, Work) of
        ok ->
            WorkRef = {work_ref, Bucket,  WorkId},
            {reply, {ok, WorkRef}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    ?LOG_WARNING("unhandled call: ~p", [Request]),
    {reply, {error, not_implemented}, State}.


handle_cast(Msg, State) ->
    ?LOG_WARNING("unhandled cast: ~p", [Msg]),
    {noreply, State}.


handle_info(work, #state{} = State) ->
    ?LOG_INFO("Fetching work."),
    ok = process_work(State),

    %% Reschedule work.
    ok = schedule_work(),

    {noreply, State};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ?LOG_DEBUG("Terminating."),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
schedule_work() ->
    %% TODO if we get timeout or overload from Riak we should use an
    %% exponential backoff strategy instead of a fixed time here
    {ok, _} = timer:send_after(1000, work),
    ok.


process_work(State) ->
    BackendMod = State#state.backend,
    Bucket = State#state.bucket,
    Symbolics = State#state.symbolics,
    Reference = State#state.reference,

    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert
    %% case)
    %% is not as bad as skipping an item (delete case.)
    %%

    Fun = fun({WorkId, Items}, ToDelete0) ->
        ?LOG_INFO("Found work to be performed: ~p", [WorkId]),

        InnerFun = fun
            (LastItem, {false, ItemsCompleted}) ->
                %% Don't iterate if the last item wasn't completed.
                ?LOG_INFO("Not attempting next item, since last failed."),
                {false, ItemsCompleted ++ [LastItem]};

            ({ItemId, Item, undefined} = LastItem, {true, ItemsCompleted}) ->
                ?LOG_INFO("Found work item to be performed: ~p", [Item]),
                %% Destructure work to be performed.
                {Node, Module, Function, Args0} = Item,

                %% Attempt to perform work.
                try
                    %% Replace symbolic terms in the work item.
                    Args = lists:map(fun(Arg) ->
                        case Arg of
                            {symbolic, Symbolic} ->
                                case dict:find(Symbolic, Symbolics) of
                                    error ->
                                        Arg;
                                    {ok, Value} ->
                                        Value
                                end;
                            _ ->
                                Arg
                        end
                    end, Args0),

                    ?LOG_DEBUG("Trying to perform work: rpc to ~p", [Node]),
                    ?LOG_DEBUG(
                        "Trying to perform work: => ~p:~p with args ~p",
                        [Module, Function, Args]
                    ),

                    Result = rpc:call(Node, Module, Function, Args),

                    ?LOG_DEBUG("Got result: ~p", [Result]),

                    %% Update item.
                    NewItems = lists:keyreplace(ItemId, 1, Items, {ItemId, Item, Result}),
                    case BackendMod:update(Reference, Bucket, WorkId, NewItems) of
                        ok ->
                            ?LOG_DEBUG("Updated item."),
                            {true, ItemsCompleted ++ [LastItem]};
                        {error, Reason} ->
                            ?LOG_DEBUG("Writing failed: ~p", [Reason]),
                            {false, ItemsCompleted ++ [LastItem]}
                    end
                catch
                    _:Error ->
                        ?LOG_ERROR("Got exception: ~p", [Error]),
                        {false, ItemsCompleted ++ [LastItem]}
                end;

            ({_, Item, _} = LastItem, {true, ItemsCompleted}) ->
                ?LOG_INFO("Found work item to be performed: ~p", [Item]),
                ?LOG_DEBUG("Work already performed, advancing to next item."),
                {true, ItemsCompleted ++ [LastItem]}
        end,

        {ItemCompleted, _} = lists:foldl(InnerFun, {true, []}, Items),

        %% Only remove the work when all of the work items are done.
        case ItemCompleted of
            true ->
                %% We made it through the entire list with a result for
                %% everything, remove.
                ?LOG_DEBUG("work ~p completed!", [WorkId]),
                ToDelete0 ++ [WorkId];
            false ->
                ?LOG_DEBUG("work ~p NOT YET completed!", [WorkId]),
                ToDelete0
        end
    end,

    AllCompleted = BackendMod:fold(Reference, Bucket, Fun, []),

    ?LOG_DEBUG("Attempting to delete completed work: ~p", [AllCompleted]),

    %% Delete items outside of iterator to ensure delete is safe.
    ok = BackendMod:delete_all(Reference, Bucket, AllCompleted),
    ok.


%% @private
get_db_connection() ->
    Host = reliable_config:riak_host(),
    Port = reliable_config:riak_port(),

    {ok, Conn} = riakc_pb_socket:start_link(Host, Port),
    pong = riakc_pb_socket:ping(Conn),
    ?LOG_DEBUG("Got connection to Riak: ~p", [Conn]),
    Conn.