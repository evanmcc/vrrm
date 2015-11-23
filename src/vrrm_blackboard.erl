-module(vrrm_blackboard).

-behavior(vrrm_replica).

%% behavior api
-export([
         init/1,
         terminate/2
        ]).

%% states
-export([
         accepting/2
        ]).

-define(S, #vrrm_blackboard_state).

-record(vrrm_blackboard_state,
        {
          board = #{} :: #{}
        }).

init(_) ->
    {ok, accepting, ?S{}}.

terminate(_Reason, _State) ->
    ok.

accepting({get, Key}, ?S{board = Board} = State) ->
    Reply =
        case maps:find(Key, Board) of
            error ->
                not_found;
            {ok, Val} ->
                Val
        end,
    {reply, Reply, accepting, State};
accepting({put, Key, Val}, ?S{board = Board} = State) ->
    Board1 = maps:put(Key, Val, Board),
    {reply, ok, accepting, State?S{board = Board1}}.

