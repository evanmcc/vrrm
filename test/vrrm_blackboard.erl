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
         handle_event/5
        ]).

-define(D, #vrrm_blackboard_data).

-record(vrrm_blackboard_data,
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
    {ok, accepting, ?D{}}.

terminate(_Reason, _State) ->
    ok.

%% no side effects here
serialize(Data) ->
    term_to_binary(Data).

deserialize(Data) ->
    binary_to_term(Data).

handle_event(call, {get, Key}, _, accepting, ?D{board = Board}) ->
    Reply =
        case maps:find(Key, Board) of
            error ->
                not_found;
            {ok, Val} ->
                Val
        end,
    {no_log, [{reply, Reply}]};
handle_event(call, {put, Key, Val}, _, accepting, ?D{board = Board} = State) ->
    Board1 = maps:put(Key, Val, Board),
    {next_state, accepting, State?D{board = Board1}, [{reply, ok}]}.
