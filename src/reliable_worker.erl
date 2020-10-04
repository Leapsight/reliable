-module(reliable_worker).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-record(state, {
    bucket                  ::  binary(),
    backend                 ::  module(),
    reference               ::  reference() | pid(),
    symbolics               ::  dict:dict(),
    fetch_work_backoff      ::  backoff:backoff(),
    fetch_work_timer        ::  reference() | undefined
}).

%% should be some sort of unique term identifier.
-type work_id() :: binary().

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
    WorkId :: work_id()
}.

-export_type([work_ref/0]).
-export_type([work_id/0]).
-export_type([work_item_id/0]).
-export_type([work_item_result/0]).
-export_type([work_item/0]).


%% API
-export([start_link/2]).
-export([enqueue/2]).
-export([enqueue/3]).
-export([status/1]).
-export([status/2]).

%% gen_server callbacks
-export([init/1,
         handle_continue/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).



%% =============================================================================
%% API
%% =============================================================================



start_link(Name, Bucket) ->
    gen_server:start({local, Name}, ?MODULE, [Bucket], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkerRef :: work_ref()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status(WorkRef) ->
    status(WorkRef, 5000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status(WorkerRef :: work_ref(), timeout()) ->
    {in_progress, Info :: map()}
    | {failed, Info :: map()}
    | {error, not_found | any()}.

status({work_ref, Instance, WorkId}, Timeout) ->
    Instance = binary_to_atom(Instance, utf8),
    gen_server:call(Instance, {status, WorkId}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(work(), binary() | undefined) ->
    {ok, work_ref()} | {error, term()}.

enqueue(Work, PartitionKey) ->
    enqueue(Work, PartitionKey, 30000).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enqueue(work(), binary() | undefined, timeout()) ->
    {ok, work_ref()} | {error, term()}.

enqueue(Work, PartitionKey, Timeout) ->
    Instance = binary_to_atom(reliable_config:partition(PartitionKey), utf8),
    gen_server:call(Instance, {enqueue, Work}, Timeout).



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


handle_continue(schedule_work, State0) ->
    State1 = schedule_work(State0),
    {noreply, State1}.


handle_call(
    {enqueue, {WorkId, _} = Work}, From, #state{bucket = Bucket} = State) ->
    %% TODO: Deduplicate here.
    %% TODO: Replay once completed.
    ?LOG_INFO("Enqueuing work: ~p, instance: ~p", [Work, Bucket]),

    BackendMod = State#state.backend,
    Ref = State#state.reference,

    case BackendMod:enqueue(Ref, Bucket, Work) of
        ok ->
            WorkRef = {work_ref, Bucket, WorkId},
            _ = gen_server:reply(From, {ok, WorkRef}),
            ok = reliable_event_manager:notify({reliable, scheduled, WorkRef}),
            {noreply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({status, WorkId}, _From, State) ->
    BackendMod = State#state.backend,
    Ref = State#state.reference,
    Bucket = State#state.bucket,

    Result = case BackendMod:get(Ref, Bucket, WorkId) of
        {ok, WorkItems} ->
            Remaining = lists:sum([1 || {_, _, undefined} <- WorkItems]),
            Info = #{
                work_id => WorkId,
                instance => Bucket,
                items => length(WorkItems),
                remaining_items => Remaining
            },
            {in_progress, Info};
        {error, _} = Error ->
            Error
    end,

    {reply, Result, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("unhandled call: ~p", [Request]),
    {reply, {error, not_implemented}, State}.


handle_cast(Msg, State) ->
    ?LOG_WARNING("unhandled cast: ~p", [Msg]),
    {noreply, State}.


handle_info(
    {timeout, Ref, fetch_work}, #state{fetch_work_timer = Ref} = State) ->
    try process_work(State) of
        ok ->
            State1 = schedule_work(succeed, State),
            {noreply, State1}
    catch
        error:Reason when Reason == overload orelse Reason == timeout ->
            State1 = schedule_work(fail, State),
            {noreply, State1}
    end;

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled info=~p, state=~p", [Info, State]),
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
schedule_work(State) ->
    Floor = 1000,
    Ceiling = 60000,
    B = backoff:type(
        backoff:init(Floor, Ceiling, self(), fetch_work),
        jitter
    ),
    State#state{
        fetch_work_backoff = B,
        fetch_work_timer = backoff:fire(B)
    }.


schedule_work(succeed, #state{fetch_work_backoff = B0} = State) ->
    {_, B1} = backoff:succeed(B0),
    State#state{
        fetch_work_backoff = B1,
        fetch_work_timer = backoff:fire(B1)
    };

schedule_work(fail, #state{fetch_work_backoff = B0} = State) ->
    {_, B1} = backoff:fail(B0),
    State#state{
        fetch_work_backoff = B1,
        fetch_work_timer = backoff:fire(B1)
    }.



%% @private
process_work(State) ->
    ?LOG_INFO("Fetching work."),
    Mod = State#state.backend,
    Bucket = State#state.bucket,
    Ref = State#state.reference,
    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert
    %% case)
    %% is not as bad as skipping an item (delete case.)

    %% We do not use the continuation, we simply query again on the next
    %% scheduled run.

    Acc0 = {[], State},
    Opts = #{max_results => 100},

    {Acc1, _Cont} = Mod:fold(Ref, Bucket, fun process_work/2, Acc0, Opts),

    {Completed, State} = Acc1,
    ?LOG_DEBUG("Attempting to delete completed work: ~p", [Completed]),

    _ = lists:foreach(
        fun(WorkId) ->
            %% Delete items outside of iterator to ensure delete is safe.
            ok = Mod:delete(Ref, Bucket, WorkId),
            %% Notify subscribers
            WorkRef = {work_ref, Bucket, WorkId},
            ok = reliable_event_manager:notify({reliable, completed, WorkRef})
        end,
        Completed
    ),

    ok.


%% @private
get_db_connection() ->
    Host = reliable_config:riak_host(),
    Port = reliable_config:riak_port(),

    {ok, Conn} = riakc_pb_socket:start_link(Host, Port),
    pong = riakc_pb_socket:ping(Conn),
    ?LOG_DEBUG("Got connection to Riak: ~p", [Conn]),
    Conn.


process_work({WorkId, Items}, {Acc, State}) ->
    ?LOG_DEBUG("Found work to be performed: ~p", [WorkId]),

    %% Only remove the work when all of the work items are done.
    ItemAcc = {true, [], WorkId, Items, State},
    case lists:foldl(fun process_work_items/2, ItemAcc, Items) of
        {true, _, _, _, _} ->
            %% We made it through the entire list with a result for
            %% everything, remove.
            ?LOG_DEBUG("work ~p completed!", [WorkId]),
            {Acc ++ [WorkId], State};
        {false, _, _, _, _} ->
            ?LOG_DEBUG("work ~p NOT YET completed!", [WorkId]),
            {Acc, State}
    end.


%% @private
process_work_items(LastItem, {false, Completed, WorkId, Items, State}) ->
    %% Don't iterate if the last item wasn't completed.
    ?LOG_INFO("Not attempting next item, since last failed."),
    {false, Completed ++ [LastItem], WorkId, Items, State};

process_work_items(
    {ItemId, Item, undefined} = LastItem,
    {true, Completed, WorkId, Items, State}) ->
    ?LOG_INFO("Found work item to be performed: ~p", [Item]),
    %% Destructure work to be performed.
    Symbolics = State#state.symbolics,
    BackendMod = State#state.backend,
    Bucket = State#state.bucket,
    Reference = State#state.reference,

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

        ?LOG_DEBUG(
            "Trying to perform work; node=~p, module=~p, "
            "function=~p, args=~p",
            [Node, Module, Function, Args]
        ),

        Result = rpc:call(Node, Module, Function, Args),

        ?LOG_DEBUG("Got result: ~p", [Result]),

        %% Update item.
        NewItems = lists:keyreplace(
            ItemId, 1, Items, {ItemId, Item, Result}),

        Result = BackendMod:update(
            Reference, Bucket, WorkId, NewItems
        ),

        case Result of
            ok ->
                ?LOG_DEBUG("Updated item."),
                {true, Completed ++ [LastItem], WorkId, Items, State};
            {error, Reason} ->
                ?LOG_DEBUG("Writing failed: ~p", [Reason]),
                {false, Completed ++ [LastItem], WorkId, Items, State}
        end
    catch
        _:Error ->
            ?LOG_ERROR("Got exception: ~p", [Error]),
            {false, Completed ++ [LastItem], WorkId, Items, State}
    end;

process_work_items(
    {_, Item, _} = LastItem, {true, Completed, WorkId, Items, State}) ->
    ?LOG_INFO("Found work item to be performed: ~p", [Item]),
    ?LOG_DEBUG("Work already performed, advancing to next item."),
    {true, Completed ++ [LastItem], WorkId, Items, State}.

