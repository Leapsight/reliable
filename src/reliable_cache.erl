-module(reliable_cache).

-export([put/1]).
-export([has/1]).




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec put(WorkId :: any()) -> ok.

put(WorkId) ->
    cache:put(?MODULE, WorkId, completed).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec has(WorkId :: any()) -> boolean().

has(WorkId) ->
    cache:has(?MODULE, WorkId).



