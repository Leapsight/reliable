-module(reliable_storage_backend).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         enqueue/1]).

%% gen_server callbacks
-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2, 
         terminate/2,  
         code_change/3]).

-define(BACKEND, reliable_dets_storage_backend).

-record(state, {reference, symbolics}).

%% should be some sort of unique term identifier.
-type work_id() :: term().

%% MFA, and a node to actually execute the RPC at.
-type work_item() :: {node(), module(), function(), [term()]}.

%% the result of any work performed.
-type work_item_result() :: term(). 

%% identifier for the work item.
-type work_item_id() :: integer().

%% the work.
-type work() :: {work_id(), [{work_item_id(), work_item(), work_item_result()}]}.

%% API

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

-spec enqueue(work()) -> ok | {error, term()}.

enqueue(Work) ->
    gen_server:call(?MODULE, {enqueue, Work}).

%% gen_server callbacks

init([]) ->
    error_logger:format("~p: initializing.", [?MODULE]),

    %% Initialize symbolic variable dict.
    Symbolics0 = dict:new(),

    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    error_logger:format("~p: got connection to Riak: ~p", [?MODULE, RiakcPid]),
    pong = riakc_pb_socket:ping(RiakcPid),

    Symbolics = dict:store(riakc, RiakcPid, Symbolics0),

    %% Schedule process to look for work.
    schedule_work(),

    %% Open storage.
    case ?BACKEND:init() of
        {ok, Reference} ->
            {ok, #state{symbolics=Symbolics, reference=Reference}};
        {error, Reason} ->
            {stop, {error, Reason}}
    end.

handle_call({enqueue, Work}, _From, #state{reference=Reference}=State) ->
    %% TODO: Deduplicate here.
    %% TODO: Replay once completed.
    error_logger:format("~p: enqueuing work: ~p", [?MODULE, Work]),

    case ?BACKEND:enqueue(Reference, Work) of 
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    error_logger:format("~p: unhandled call: ~p", [?MODULE, Request]),
    {reply, {error, not_implemented}, State}.

handle_cast(Msg, State) ->
    error_logger:format("~p: unhandled cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(work, #state{symbolics=Symbolics, reference=Reference}=State) ->
    % error_logger:format("~p: looking for work.", [?MODULE]),

    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert case) 
    %% is not as bad as skipping an item (delete case.)
    %%
    ItemsToDelete = ?BACKEND:fold(Reference, fun({WorkId, WorkItems}, ItemsToDelete0) ->
        error_logger:format("~p: found work to be performed: ~p", [?MODULE, WorkId]),

        {ItemCompleted, _} = lists:foldl(fun({WorkItemId, WorkItem, WorkItemResult}=LastWorkItem, {LastWorkItemCompleted0, WorkItemsCompleted}) ->
            %% Don't iterate if the last item wasn't completed.
            case LastWorkItemCompleted0 of 
                false ->
                    error_logger:format("~p: not attempting next item, since last failed.", [?MODULE]),
                    {LastWorkItemCompleted0, WorkItemsCompleted ++ [LastWorkItem]};
                true ->
                    error_logger:format("~p: found work item to be performed: ~p", [?MODULE, WorkItem]),

                    case WorkItemResult of 
                        undefined ->
                            %% Destructure work to be performed.
                            {Node, Module, Function, Args0} = WorkItem,

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

                                error_logger:format("~p: trying to perform work: rpc to ~p", [?MODULE, Node]),
                                error_logger:format("~p: trying to perform work: => ~p:~p with args ~p", [?MODULE, Module, Function, Args]),
                                Result = rpc:call(Node, Module, Function, Args),
                                error_logger:format("~p: got result: ~p", [?MODULE, Result]),

                                %% Update item.
                                NewWorkItems = lists:keyreplace(WorkItemId, 1, WorkItems, {WorkItemId, WorkItem, Result}),
                                case ?BACKEND:update(Reference, WorkId, NewWorkItems) of
                                    ok ->
                                        error_logger:format("~p: updated item.", [?MODULE]),
                                        {true, WorkItemsCompleted ++ [LastWorkItem]};
                                    {error, Reason} ->
                                        error_logger:format("~p: writing failed: ~p", [?MODULE, Reason]),
                                        {false, WorkItemsCompleted ++ [LastWorkItem]}
                                end
                            catch
                                _:Error ->
                                    error_logger:format("~p: got exception: ~p", [?MODULE, Error]),
                                    {false, WorkItemsCompleted ++ [LastWorkItem]}
                            end;
                        _ ->
                            error_logger:format("~p: work already performed, advancing to next item.", [?MODULE]),
                            {true, WorkItemsCompleted ++ [LastWorkItem]}
                    end
            end
        end, {true, []}, WorkItems),

        %% Only remove the work when all of the work items are done.
        case ItemCompleted of
            true ->
                %% We made it through the entire list with a result for everything, remove.
                error_logger:format("~p: work ~p completed!", [?MODULE, WorkId]),
                ItemsToDelete0 ++ [WorkId];
            false ->
                error_logger:format("~p: work ~p NOT YET completed!", [?MODULE, WorkId]),
                ItemsToDelete0
        end
    end, []),

    error_logger:format("~p: attempting to delete keys: ~p", [?MODULE, ItemsToDelete]),

    %% Delete items outside of iterator to ensure delete is safe.
    ok = ?BACKEND:delete_all(Reference, ItemsToDelete),

    %% Reschedule work.
    schedule_work(),

    {noreply, State};

handle_info(Info, State) ->
    error_logger:format("~p: unhandled info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    error_logger:format("~p: terminating.", [?MODULE]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% @private
schedule_work() ->
    timer:send_after(1000, work).