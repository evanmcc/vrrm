-module(vrrm_manager).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         start_quorum/4,
         stop_quorum/1,
         quorum_status/1
        ]).

%% gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(SERVER, ?MODULE).

-record(state,
        {
          nodes = set:new() :: sets:set(),
          quora = #{} :: #{term() => maps:map()}
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_quorum(Name, Mod, ModArgs, Nodes) ->
    gen_server:call(?SERVER, {start_quorum, Name, Mod, ModArgs, Nodes}).

stop_quorum(Name) ->
    gen_server:call(?SERVER, {stop_quorum, Name}).

quorum_status(Name) ->
    gen_server:call(?SERVER, {quorum_status, Name}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    erlang:process_flag(trap_exit, true),
    start_timer(),
    Nodes = lists:foldl(fun(E, A) -> sets:add_element(E, A) end,
                        sets:new(), nodes()),
    {ok, #state{nodes = Nodes}}.

handle_call({start_quorum, Name, Mod, ModArgs, RequestedNodes}, _From,
            State = #state{nodes = Nodes,
                           quora = Quora}) ->
    NodeList =
        case RequestedNodes of
            all ->
                sets:to_list(Nodes);
            %% it'd be a good idea to check that List is a subset of Nodes
            List when is_list(List) ->
                List
        end,
    ReplyWait = application:get_env(vrrm, manager_reply_wait_time, 150),
    case Quora of
        #{Name := _Quorum} ->
            %% modify should be another operation
            {reply, already_started, State};
        _ ->
            Replies =
                [{Node, (catch gen_server:call({?SERVER, Node}, list_prep, ReplyWait))}
                 || Node <- NodeList],
            %% now that we've got an active list
            FilteredList = [Node || {Node, ok} <- Replies],
            lager:info("filtered list: ~p ~p", [FilteredList, Replies]),
            QuorumNodes0 =
                [begin
                     {ok, Pid} = gen_server:call({?SERVER, Node},
                                                 {start_quorum_member, Name, Mod, ModArgs},
                                                 ReplyWait),
                     Pid
                 end
                 || Node <- FilteredList],
            {ok, Pid} = vrrm_replica:start_link(Mod, ModArgs, true, #{}),
            QuorumNodes = [Pid | QuorumNodes0],
            lager:info("quorum nodes: ~p", [QuorumNodes]),
            vrrm_replica:initial_config(Pid, QuorumNodes),
            [gen_server:call({?SERVER, Node},
                             {config_quorum, Name, QuorumNodes},
                             ReplyWait)
             || Node <- FilteredList],
            Q2 = Quora#{Name => #{mod => Mod,
                                  mod_args => ModArgs,
                                  pid => Pid,
                                  nodes => QuorumNodes}},
            {reply, ok, State#state{quora = Q2}}
    end;
handle_call(list_prep, _From, State) ->
    {reply, ok, State};
handle_call({start_quorum_member, Name, Mod, ModArgs}, _From,
            State = #state{quora = Quora}) ->
    {ok, Pid} = vrrm_replica:start_link(Mod, ModArgs, true, #{}),
    Q2 = Quora#{Name => #{mod => Mod,
                          mod_args => ModArgs,
                          pid => Pid}},
    {reply, {ok, Pid}, State#state{quora = Q2}};
handle_call({config_quorum, Name, NodeList}, _From,
            State = #state{quora = Quora}) ->
    #{Name := Quorum} = Quora,
    #{pid := Pid} = Quorum,
    Quora1 = Quora#{Name := Quorum#{nodes => NodeList}},
    vrrm_replica:initial_config(Pid, NodeList),
    {reply, ok, State#state{quora = Quora1}};
handle_call({quorum_status, Name}, _From,
            State = #state{quora = Quora}) ->
    lager:info("quora ~p", [Quora]),
    case Quora of
        #{Name := Quorum} ->
            #{pid := Pid} = Quorum,
            Reply =
                case vrrm_replica:get_config(Pid) of
                    {error, not_primary, Primary} ->
                        lager:info("primary? ~p", [Primary]),
                        {ok, Status} = vrrm_replica:get_status(Primary),
                        Status;
                    {ok, L} when is_list(L) ->
                        {ok, Status} = vrrm_replica:get_status(Pid),
                        Status
                end,
            %% get the state from the leader
            {reply, Reply, State};
        _ ->
            {reply, {error, no_such_quorum, Quora}, State}
    end;
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

%% we should have another tick that reaps dead nodes after we haven't
%% heard from them in N ticks
handle_info(update_nodes, State = #state{nodes = Nodes}) ->
    NewNodes = lists:foldr(
                 fun (Elt, Acc) ->
                         sets:add_element(Elt, Acc)
                 end,
                 Nodes, nodes()),
    start_timer(),
    {noreply, State#state{nodes = NewNodes}};
handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_timer() ->
    Time = application:get_env(vrrm, manager_update_interval, 1000),
    erlang:send_after(Time, self(), update_nodes).
