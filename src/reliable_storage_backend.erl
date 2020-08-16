-module(reliable_storage_backend).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

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

-define(BACKEND, reliable_riak_storage_backend).

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

-export_type([work_item/0]).

%% API

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

-spec enqueue(work()) -> ok | {error, term()}.

enqueue(Work) ->
    gen_server:call(?MODULE, {enqueue, Work}).

%% gen_server callbacks

init([]) ->
    ?LOG_INFO("~p: initializing.", [?MODULE]),

    %% Initialize symbolic variable dict.
    Symbolics0 = dict:new(),

    {ok, RiakcPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    ?LOG_INFO("~p: got connection to Riak: ~p", [?MODULE, RiakcPid]),
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
    ?LOG_INFO("~p: enqueuing work: ~p", [?MODULE, Work]),

    case ?BACKEND:enqueue(Reference, Work) of
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    ?LOG_INFO("~p: unhandled call: ~p", [?MODULE, Request]),
    {reply, {error, not_implemented}, State}.

handle_cast(Msg, State) ->
    ?LOG_INFO("~p: unhandled cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(work, #state{symbolics=Symbolics, reference=Reference}=State) ->
    % ?LOG_INFO("~p: looking for work.", [?MODULE]),

    %% Iterate through work that needs to be done.
    %%
    %% Probably inserting when holding the iterator is a problem too?
    %% Not sure. I'm assuming not for now since yielding the same item (insert case)
    %% is not as bad as skipping an item (delete case.)
    %%
    ItemsToDelete = ?BACKEND:fold(Reference, fun({WorkId, WorkItems}, ItemsToDelete0) ->
        ?LOG_INFO("~p: found work to be performed: ~p", [?MODULE, WorkId]),

        {ItemCompleted, _} = lists:foldl(fun({WorkItemId, WorkItem, WorkItemResult}=LastWorkItem, {LastWorkItemCompleted0, WorkItemsCompleted}) ->
            %% Don't iterate if the last item wasn't completed.
            case LastWorkItemCompleted0 of
                false ->
                    ?LOG_INFO("~p: not attempting next item, since last failed.", [?MODULE]),
                    {LastWorkItemCompleted0, WorkItemsCompleted ++ [LastWorkItem]};
                true ->
                    ?LOG_INFO("~p: found work item to be performed: ~p", [?MODULE, WorkItem]),

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

                                ?LOG_INFO("~p: trying to perform work: rpc to ~p", [?MODULE, Node]),
                                ?LOG_INFO("~p: trying to perform work: => ~p:~p with args ~p", [?MODULE, Module, Function, Args]),
                                Result = rpc:call(Node, Module, Function, Args),
                                ?LOG_INFO("~p: got result: ~p", [?MODULE, Result]),

                                %% Update item.
                                NewWorkItems = lists:keyreplace(WorkItemId, 1, WorkItems, {WorkItemId, WorkItem, Result}),
                                case ?BACKEND:update(Reference, WorkId, NewWorkItems) of
                                    ok ->
                                        ?LOG_INFO("~p: updated item.", [?MODULE]),
                                        {true, WorkItemsCompleted ++ [LastWorkItem]};
                                    {error, Reason} ->
                                        ?LOG_INFO("~p: writing failed: ~p", [?MODULE, Reason]),
                                        {false, WorkItemsCompleted ++ [LastWorkItem]}
                                end
                            catch
                                _:Error ->
                                    ?LOG_ERROR("~p: got exception: ~p", [?MODULE, Error]),
                                    {false, WorkItemsCompleted ++ [LastWorkItem]}
                            end;
                        _ ->
                            ?LOG_INFO("~p: work already performed, advancing to next item.", [?MODULE]),
                            {true, WorkItemsCompleted ++ [LastWorkItem]}
                    end
            end
        end, {true, []}, WorkItems),

        %% Only remove the work when all of the work items are done.
        case ItemCompleted of
            true ->
                %% We made it through the entire list with a result for everything, remove.
                ?LOG_INFO("~p: work ~p completed!", [?MODULE, WorkId]),
                ItemsToDelete0 ++ [WorkId];
            false ->
                ?LOG_INFO("~p: work ~p NOT YET completed!", [?MODULE, WorkId]),
                ItemsToDelete0
        end
    end, []),

    ?LOG_INFO("~p: attempting to delete keys because work complete: ~p", [?MODULE, ItemsToDelete]),

    %% Delete items outside of iterator to ensure delete is safe.
    ok = ?BACKEND:delete_all(Reference, ItemsToDelete),

    %% Reschedule work.
    schedule_work(),

    {noreply, State};

handle_info(Info, State) ->
    ?LOG_INFO("~p: unhandled info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ?LOG_INFO("~p: terminating.", [?MODULE]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% @private
schedule_work() ->
    timer:send_after(1000, work).