-module(vrrm_blackboard).

-behavior(vrrm_replica).

%% API
-export([
         start_link/0,
         get/2,
         put/3,
         cas/4
        ]).

%% behavior api
-export([
         init/1,
         terminate/2,
         serialize/1,
         deserialize/1,
         handle_info/2
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

%%% API

start_link() ->
    vrrm_replica:start_link(?MODULE, []).

get(Primary, Key) ->
    vrrm_cli:request(Primary, {get, Key}).

put(Primary, Key, Val) ->
    vrrm_cli:request(Primary, {put, Key, Val}).

cas(Primary, Key, Current, New) ->
    vrrm_cli:request(Primary, {cas, Key, Current, New}).

%%% behavior

init(_) ->
    {ok, accepting, ?S{}}.

terminate(_Reason, _State) ->
    ok.

%% no side effects here
serialize(State) ->
    State.

deserialize(State) ->
    State.

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


handle_info(_, State) ->
    {next_state, accepting, State}.
