-module(vrrm_ha_sup).

-behavior(vrrm_replica).

%% this code is obviously influenced by reading supervisor.erl, but
%% it's massively simplified for now.

%% since this is a non-deterministic service, our major strategy for
%% staying in sync is going to have to be dying early when things
%% don't go as we expect.  this is not ideal, I suppose, but it isn't
%% clear how we manage, otherwise.

-define(S, #vrrm_ha_sup_state).

-type strategy() :: one_for_one.

-record(vrrm_ha_sup_state,
        {
          %% supervisor state
          strategy = one_for_one :: strategy(),
          children = [] :: [child()],
          intensity :: pos_integer(),
          period :: non_neg_integer(),
          mod :: atom(),
          args :: [term()],

          %% quorum state, do we need this?
          primary :: pid(),

          %% client
          client :: vrrm_cli:cli()
        }).

-record(child,
         {
          name :: atom(),
          pid :: pid(),
          mfa :: {atom(), atom(), [term()]},
          restart :: temporary | permanent,
          shutdown_timeout :: pos_integer(),
          supervisor :: boolean()
         }).

-type child() :: #child{}.

-callback init(Args :: term()) ->
    {ok, term()}.

%%% API
-export([
         start_link/2, %% start_link/3,
         start_child/1, %% restart_child/2,
         %%delete_child/2,
         terminate_child/1,
         which_children/0,
         count_children/0 %% ,
         %% check_childspecs/1
        ]).

%%% replica callbacks
-export([
         init/1,
         handle_info/2,
         serialize/1,
         deserialize/1,
         terminate/2,

         %% states
         starting/2,
         accepting/2
        ]).

%%% API implmentation

start_link(Mod, Args) ->
    vrrm_replica:start_link(?MODULE, [Mod, Args], true, []).

%% for the moment, we only allow one ha_sup per node

start_child(Spec) ->
    vrrm_cli:request(vrrm_ha_sup, {start_child, Spec}).

%% dynamically terminate child quorum and do not restart
terminate_child(Name) ->
    vrrm_cli:request(vrrm_ha_sup, {terminate, Name}).

which_children() ->
    vrrm_cli:request(vrrm_ha_sup, which_children).

count_children() ->
    {ok, Children} = which_children(),
    length(Children).


%%% callback implementation
init({Mod, Args}) ->
    process_flag(trap_exit, true),
    register(?MODULE, self()),
    case Mod:init(Args) of
        {ok, {SupFlags, StartSpec}} ->
            %% bomb out unless one_for one for now
            {one_for_one, Intensity, Period} = SupFlags,
            Children = init_children(StartSpec),
            {ok, starting,
             ?S{strategy = one_for_one,
                intensity = Intensity,
                period = Period,
                mod = Mod,
                args = Args,
                %% convert into a map by name
                children = maps:from_list(Children),
                client = vrrm_cli:new()
               }};
        _Err ->
            {stop, {bad_init, Mod, Args, _Err}}
    end.

init_children(Children) ->
    [begin
         {ok, Pid} = spawn_link(M, F, A),
         SupP = SupType =:= supervisor,
         {Name,
          #child{name = Name,
                 pid = Pid,
                 mfa = {M, F, A},
                 restart = RestartType,
                 shutdown_timeout = ShutdownTime,
                 supervisor = SupP
                }}
     end
     || {Name, {M, F, A}, RestartType,
         ShutdownTime, SupType, _Mods}
            <- Children].

serialize(State) ->
    State.

deserialize(State) ->
    State.

terminate(Reason, State) ->
    terminate_children(Reason, State?S.children),
    ok.

starting(_Msg, ?S{client = Cli} = State) ->
    %% bootstrap into the quorum, or wait for enough nodes
    {ok, Nodes} = wait_for_min_quorum_size(30),
    case quorum_exists(Nodes) of
        {true, Primary} ->
            Config = vrrm_replica:get_config(Primary),
            NewConfig = lists:sort([self() | Config]),
            {ok, Cli1} = vrrm_cli:reconfigure(Primary, NewConfig, Cli);
        {false, Pids} ->
            Config = lists:sort([self() | Pids]),
            [Primary|_] = Config,
            Cli1 = Cli,
            case self() of
                Primary ->
                    %% assuming no bad races, this should work
                    [vrrm_replica:initial_config(R, Config)
                     || R <- Config];
                _ ->
                    ok
            end
    end,
    {next_state, accepting,
     State?S{primary = Primary, client = Cli1}}.

quorum_exists(Nodes) ->
    %% ideally this would be in parallel
    Replies = [gen_server:call({?MODULE, Node}, get_config)
               || Node <- Nodes],
    case [R || R <- Replies, R /= not_primary] of
        %% this should only happen on a live cluster
        [Config] ->
            [Primary|_] = Config,
            {true, Primary};
        [] ->
            %% ideally this would also be parallel
            Pids =
                [rpc:call(Node, erlang, whereis, [vrrm_ha_sup])
                 || Node <- Nodes],
            {false, Pids}
    end.

wait_for_min_quorum_size(0) ->
    {error, timeout};
wait_for_min_quorum_size(N) ->
    Nodes = nodes(),
    case length(Nodes) of
        L when L < 2 ->
            timer:sleep(timer:seconds(1)),
            wait_for_min_quorum_size(N - 1);
        _TooFew ->
            {ok, Nodes}
    end.

accepting({start_child, Spec}, ?S{children = Children} = State) ->
    %% TODO: need better handling of init failure throughout
    %% TODO: check for duplicate names
    [{Name, Child}] = init_children([Spec]),
    Children1 = maps:put(Name, Child, Children),
    {reply, ok, accepting, State?S{children = Children1}};
accepting({terminate_child, Name}, ?S{children = Children} = State) ->
    case maps:find(Name, Children) of
        {ok, #child{pid = Pid}} ->
            %% I think in an ideal world we would do this in another
            %% pid (with a timeout?) since this can block
            vrrm_replica:stop(Pid),
            Children1 = maps:remove(Name, Children),
            {reply, ok, accepting, State?S{children = Children1}};
        _ ->
            {reply, {error, not_found}, accepting, State}
    end;
accepting(which_children, ?S{children = Children} = State) ->
    {reply, maps:to_list(Children), accepting, State}.

handle_info({'EXIT', Pid, Info},
            ?S{children = Children} = State) ->
    lager:info("supervised process ~p exited with reason ~p",
               [Pid, Info]),
    %% try once then die, eventually deal with restart intensity
    [Child] = [C || #child{pid = P} = C <- maps:to_list(Children),
                    P =:= Pid],
    #child{name = Name, mfa = {M, F, A}} = Child,
    Pid1 = spawn_link(M, F, A),
    Children1 = maps:put(Name, Child#child{pid = Pid1}, Children),
    {next_state, accepting, State?S{children = Children1}}.


%%% internal functions
terminate_children(Reason, Children) ->
    [begin
         unlink(Child),
         exit(Child, Reason)
     end
     || Child <- Children].
