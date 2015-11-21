-module(vrrm_replica).

-behaviour(gen_server).

%% API
-export([
         start_link/5
        ]).

%% gen_server callbacks
-export([
         init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%%% messages
-export([
         %% async
         prepare/6,
         prepare_ok/4,
         commit/4,

         %% start_view_change/2,
         %% do_view_change/2,

         %% recovery/2,
         %% recovery_response/2,

         %% reconfiguration/2,
         %% start_epoch/2,
         %% epoch_started/2,

         %% %% sync
         %% get_state/2,
         %% get_op/2, get_op/3,
         request/4
        ]).

-define(SERVER, ?MODULE).

-record(client_requests,
        {
          address,
          requests = [] :: [{Op::integer(),
                             ReqNum::integer(),
                             Command::any()}],
          latest_req = 0 :: non_neg_integer()
        }).

-record(replica_state,
        {
          %% replication state
          primary :: boolean(),
          config :: [atom()], % nodenames
          replica :: non_neg_integer(),
          view :: non_neg_integer(),
          last_normal :: non_neg_integer(),
          status = recovering :: normal |
                                 view_change |
                                 recovering |
                                 transitioning,
          log :: [term()],
          op =  0 :: non_neg_integer(),
          commit :: non_neg_integer(),
          client_table = #{} :: #{tuple() => #client_requests{}},
          epoch = 0 :: non_neg_integer(),
          old_config :: [atom()],

          pending_replies = [] :: [],

          %% callback module state
          mod :: atom(),
          mod_state :: tuple()
        }).

-define(S, #replica_state).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(atom(), [term()], [], boolean(), #{}) ->
                        {ok, pid()} |
                        {error, Reason::term()}.
start_link(Mod, ModArgs, Config, New, Opts) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE,
                       [Mod, ModArgs, Config, New, Opts], []).

%% request is should be abstracted to the vrrm_client module, mostly.
-spec request(pid(), term(), vrrm_client:id(), non_neg_integer()) ->
                     {ok, Reply::term()} |
                     {error, Reason::term()}.
request(Primary, Command, Client, Request) ->
    gen_server:call(Primary, {request, Command, Client, Request},
                    500).


prepare(Replica, View, Command, Op, Commit, Epoch) ->
    gen_server:cast(Replica, {prepare, self(), View,
                              Command, Op, Commit, Epoch}).

prepare_ok(Replica, View, Op, Epoch) ->
    gen_server:cast(Replica, {prepare_ok, View, Op, self(), Epoch}).

commit(Replica, View, Commit, Epoch) ->
    gen_server:cast(Replica, {commit, View, Commit, Epoch}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, ModArgs, Config, New, _Opts]) ->
    case New of
        true ->
            ModState = Mod:init(ModArgs),
            {ok, ?S{config = Config,
                    mod = Mod,
                    mod_state = ModState
                   }};
        false ->
            throw(oh_god)
    end.

%% request
handle_call({request, Command, Client, Request},
            _From,
            ?S{primary = Primary, log = Log,
               commit = Commit, replica = Self,
               view = View, op = Op, epoch = Epoch,
               client_table = Table, config = Config} = State) ->
    %% are we primary? ignore (or reply not_primary?) if not
    case Primary of
        true ->
            {reply, not_primary, State};
        false ->
            %% compare with recent requests table, resend reply if
            %% already completed
            case recent(Client, Request, Table) of
                {true, Reply} ->
                    {reply, Reply, State};
                false ->
                    %% advance the op_num, add request to the log,
                    %% update client table cast prepare to other
                    %% replicas, return noreply
                    Op1 = Op + 1,
                    Log1 = [{Op1, Command, false}|Log],
                    Table1 = add_request(Client, Request, Op1, View, Table),
                    Address = lists:nth(Self, Config),
                    [vrrm_replica:prepare(Replica, self(), View,
                                          Command, Op1, Commit, Epoch)
                     || Replica <- Config, Replica /= Address],
                    {noreply, State?S{client_table = Table1,
                                      log = Log1,
                                      op = Op1}}
            end
    end;
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

%%% for all of the following message classes, if ViewNum < view_num,
%%% the message is ignored, or Epoch < epoch_num
handle_cast({prepare, Primary, View, Command, Op, Commit, Epoch},
            State0) ->
    %% is log complete? consider state transfer if not
    State = maybe_catchup(Op, State0),
    ?S{log = Log, mod = Mod, mod_state = ModState} = State,
    %% if CommitNum is higher than commit_num, do some upcalls
    {Log1, Commit1, Commands} =
        commit_uncommitted(Log, Commit),
    ModState1 = lists:foldl(fun(Cmd, MState) ->
                                    Mod:handle_event(Cmd, MState)
                            end,
                            ModState,
                            Commands),
    %% increment op number, append operation to log, update client table(?)
    Log2 = [{Op, Command, false}|Log1],
    %% reply with prepare_ok
    vrrm_replica:prepare_ok(Primary, View, Op, self(), Epoch),
    %% reset primary failure timeout
    {noreply, State?S{log = Log2, op = Op,
                      mod_state = ModState1,
                      commit = Commit1}, timer:seconds(5)};
handle_cast({prepare_ok, View, Op, _Sender, Epoch},
            ?S{commit = Commit,
               view = LocalView,
               epoch = LocalEpoch} = State)
  when Commit >= Op; LocalEpoch > Epoch; LocalView > View ->
    %% ignore these, we've already processed them.
    {noreply, State};
handle_cast({prepare_ok, View, Op, Sender, _Epoch} = Msg,
            ?S{mod = Mod, mod_state = ModState,
               client_table = Table,
               pending_replies = Pending} = State) ->
    %% do we have f replies? if no, wait for more (or timeout)
    F = f(State?S.config),
    OKs = scan_pend(prepare_ok, Op, View, Pending),
    case length(OKs) of
        N when N >= F ->
            %% update commit_num to OpNum, do Mod upcall, send {reply,
            %% v, s, x} set commit message timeout.
            Request = get_request(Op, View, Table),
            %% prob need to unwrap here, also need request record.
            {reply, Reply, ModState1} =
                Mod:handle_event(element(2, Request), ModState),
            gen_server:reply(element(3, Request), Reply),
            Table1 = add_reply(Op, View, Reply, Table),
            Pending1 = clean_pend(Op, View, Pending),
            %% do we need to check if commit == Op - 1?
            {noreply, State?S{client_table = Table1, commit = Op,
                              mod_state = ModState1,
                              pending_replies = Pending1}};
        _ ->
            %% wait for more
            %% addendum: this is bad FIXME
            Pending1 = [{Msg, Sender}|Pending],
            {noreply, State?S{pending_replies = Pending1}}
    end;
handle_cast({commit, _View, Commit, _Epoch},
            State0) ->
    %% if Commit > commit_num, do upcalls, doing state transfer if
    %% needed.
    State = maybe_catchup(Commit, State0),
    ?S{log = Log, mod = Mod, mod_state = ModState} = State,
    {Log1, Commit1, Commands} =
        commit_uncommitted(Log, Commit),
    ModState1 = lists:foldl(fun(Cmd, MState) ->
                                    Mod:handle_event(Cmd, MState)
                            end,
                            ModState,
                            Commands),

    %% reset primary failure timeout
    {noreply, State?S{log = Log1, mod_state = ModState1,
                      commit = Commit1},
     timer:seconds(5)};
%% handle_cast({start_view_change, View, Sender, Epoch},
%%             State) ->
%%     %% have we recieved f view change messages? if no store and wait
%%     %% if yes, send a do_view_change message to the new primary
%%     %% change state to view_change if not already there, and note last_normal
%% handle_cast({do_view_change, View, Log, OldView, Op, Commit, Sender, Epoch},
%%             State) ->
%%     %% have we gotten f + 1 (inclusive of self)? if no, wait
%%     %% if yes (and all new ViewNums agree) set op num to largest in
%%     %% result-set, select new log from result set member with the
%%     %% largest op_num.  set commit number to largest number in the
%%     %% result set, return status to normal, and send start_view to all
%%     %% other replicas (maybe compacting log first)
%% handle_cast({start_view, ViewNum, Log, Op, Commit, Epoch},
%%             State) ->
%%     %% replce log with Log, set op_num to highest op in the log, set
%%     %% view_num to View, if there are uncommitted operations in the
%%     %% log, send prepare_ok messages to the primary for them, commit
%%     %% any known commited and update commit number.
%% handle_cast({recovery, Sender, Nonce, Epoch},
%%             State) ->
%%     %% if status is normal, send recovery_response
%%     %% if primary, include additional information
%% handle_cast({recovery_response, View, Nonce, Log, Op, Commit, Sender, Epoch},
%%             State) ->
%%     %% have we got f + 1 incuding the primary response? if no wait
%%     %% if yes, use primary values, set to normal, restart participation
%% handle_cast({reconfiguration, Epoch, Client, Request, NewConfig},
%%             State) ->
%%     %% validate new config, Epoch == epoch, and Request is not already
%%     %% processed for this Client (only valid on primary).
%%     %% if that's all OK: insert a new, special internal operation into
%%     %% the consensus log & do the normal stuff, then stop accepting
%%     %% client requests (queue, then deal with them after
%%     %% start_epoch?). when that completes, the prepare_ok step will
%%     %% send start_epoch to new nodes
%% handle_cast({start_epoch, Epoch, Op, OldConfig, NewConfig, Sender},
%%             State) ->
%%     %% if we're normal and in Epoch, reply with epoch_started
%%     %% record Epoch, Op, and configs, set view number = 0, and state
%%     %% to transitioning.
%%     %% get up to date with state transfers to old and new nodes
%%     %% send leaving nodes epoch_started
%% handle_cast({epoch_started, Epoch, Sender}, State) ->
%%     %% if we've gotten f + 1 of these, we die.
%%     %% if we don't get enough in time, we re-send start_epoch to new nodes
handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info(timeout, State) ->
    %% if the view is current, send start_view_change to all other
    %% replicas.
    {noreply, State};
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

recent(_Client, _Request, _Table) ->
    ok.

add_request(_Client, _Request, _Op, _View, _Table) ->
    ok.

add_reply(_Op, _View, _Reply, _Table) ->
    ok.

get_request(_Op, _View, _Table) ->
    ok.

maybe_catchup(_Op, _State) ->
    ok.

commit_uncommitted(_Log, _Commit) ->
    ok.

f(Config) ->
    L = length(Config),
    (L-2) div 2.

scan_pend(_Message, _Op, _View, _Pending) ->
    ok.

clean_pend(_Op, _View, _Pending) ->
    ok.
