-module(reliable_storage_backend).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start/0,
         enqueue/1]).

%% gen_server callbacks
-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2, 
         terminate/2,  
         code_change/3]).

-define(FILENAME, "reliable-backend-data").

-record(state, {reference}).

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

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

-spec enqueue(work()) -> ok | {error, term()}.

enqueue(Work) ->
    gen_server:call({local, ?MODULE}, {work, Work}).

%% gen_server callbacks

init([]) ->
    lager:info("~p: initializing.", [?MODULE]),

    case dets:open_file(?FILENAME) of 
        {ok, Reference} ->
            {ok, #state{reference=Reference}};
        {error, Reason} ->
            {stop, {error, Reason}}
    end.

handle_call({enqueue, Work}, _From, #state{reference=Reference}=State) ->
    %% TODO: Deduplicate here.
    lager:info("~p: enqueuing work.", [?MODULE, Work]),

    case dets:insert_new(Reference, Work) of 
        true ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    lager:info("~p: unhandled call: ~p", [?MODULE, Request]),
    {reply, {error, not_implemented}, State}.

handle_cast(Msg, State) ->
    lager:info("~p: unhandled cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(work, #state{reference=Reference}=State) ->
    lager:info("~p: looking for work.", [?MODULE]),

    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert case) 
    %% is not as bad as skipping an item (delete case.)
    %%
    ItemsToDelete = dict:foldl(fun({WorkId, WorkItems}, ItemsToDelete0) ->
        lager:info("~p: found work to be performed: ~p", [?MODULE, WorkId]),

        {ItemCompleted, _} = lists:foldl(fun({WorkItemId, WorkItem, WorkItemResult}=LastWorkItem, {LastWorkItemCompleted0, WorkItemsCompleted}) ->
            %% Don't iterate if the last item wasn't completed.
            case LastWorkItemCompleted0 of 
                false ->
                    lager:info("~p: not attempting next item, since last failed.", [?MODULE]),
                    {LastWorkItemCompleted0, WorkItemsCompleted ++ [LastWorkItem]};
                true ->
                    lager:info("~p: found work item to be performed: ~p", [?MODULE, WorkItem]),

                    case WorkItemResult of 
                        undefined ->
                            %% Destructure work to be performed.
                            {Node, Module, Function, Args} = WorkItem,

                            %% Attempt to perform work.
                            try
                                lager:info("~p: trying to perform work.", [?MODULE]),
                                Result = rpc:call(Node, Module, Function, Args),
                                lager:info("~p: got result: ~p", [?MODULE, Result]),

                                %% Update item in dets.
                                NewWorkItems = lists:keyreplace(WorkItemId, 1, WorkItems, {WorkItemId, WorkItem, Result}),
                                case dets:insert(Reference, {WorkId, NewWorkItems}) of
                                    ok ->
                                        lager:info("~p: updated item in dets.", [?MODULE]),
                                        {true, WorkItemsCompleted ++ [LastWorkItem]};
                                    {error, Reason} ->
                                        lager:info("~p: writing to dets failed: ~p", [?MODULE, Reason]),
                                        {false, WorkItemsCompleted ++ [LastWorkItem]}
                                end
                            catch
                                _:Error ->
                                    lager:info("~p: got exception: ~p", [?MODULE, Error]),
                                    {false, WorkItemsCompleted ++ [LastWorkItem]}
                            end;
                        _ ->
                            lager:info("~p: work already performed, advancing to next item.", [?MODULE]),
                            {true, WorkItemsCompleted ++ [LastWorkItem]}
                    end
            end
        end, {true, []}, WorkItems),

        %% Only remove the work when all of the work items are done.
        case ItemCompleted of
            true ->
                %% We made it through the entire list with a result for everything, remove from dets.
                lager:info("~p: work ~p completed!", [?MODULE, WorkId]),
                ItemsToDelete0 ++ [WorkId];
            false ->
                lager:info("~p: work ~p NOT YET completed!", [?MODULE, WorkId]),
                ItemsToDelete0
        end
    end, [], Reference),

    %% Delete items outside of iterator to ensure delete is safe.
    lists:foreach(fun(Key) ->
        dets:delete_object(Reference, Key)
    end, ItemsToDelete),

    %% Reschedule work.
    schedule_work(),

    {noreply, State};

handle_info(Info, State) ->
    lager:info("~p: unhandled info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    lager:info("~p: terminating.", [?MODULE]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% @private
schedule_work() ->
    erlang:send_after(1000, work).