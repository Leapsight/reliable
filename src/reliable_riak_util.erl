-module(reliable_riak_util).


-export([format_error_reason/1]).



%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec format_error_reason(term()) -> term().

format_error_reason(<<"Operation type is", _/binary>>) ->
    datatype_mismatch;

format_error_reason(<<"overload">>) ->
    overload;

format_error_reason(notfound) ->
    not_found;

format_error_reason(Reason) ->
    Reason.


