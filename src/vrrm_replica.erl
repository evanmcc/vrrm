-module(vrrm_replica).

-behaviour(gen_server).

%% API
-export([
         start_link/2, start_link/4
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
         prepare/8,
         prepare_ok/4,
         commit/4,

         start_view_change/3,
         do_view_change/7,

         recovery/4,
         recovery_response/7,

         start_epoch/5,
         epoch_started/2,

         %% %% sync
         request/3, request/4,
         initial_config/2,
         get_config/1,
         reconfigure/3,
         get_mod_state/1,
         state_transfer/1,
         am_primary/1
         %% get_op/2, get_op/3,
         %% swap_node/2
        ]).

-callback init(Args::[term()]) ->
    {ok, StateName::atom(), ModState::term()} |
    {error, Reason::term()}.

%% any number of states can defined by the behavior, which must
%% express them:
%% Mod:StateName(Event::term(), ModState::term()) ->
%%     {next_state, NextStateName, UpdModState} |
%%     {reply, Reply, NextStateName, UpdModState} |
%%     {stop, UpdModState}.

-callback handle_info(Msg::term(), State::term()) ->
    {next_state, NextState::atom(), State::term()} |
    {stop, Reason::term(), State::term()}.

-callback serialize(State::term()) ->
    {ok, SerializedState::term()} | {error, Reason::term()}.

-callback deserialize(SerializedState::term()) ->
    {ok, DeserializedState::term()} | {error, Reason::term()}.

%% when we add leases, we want to be able to do no_transition
%% operations from the primary without having to consult the replicas
%% while the lease is valid.  the canoncial example here is reads in a
%% kv store. this optional callback will give a list of these states.
%% -callback no_transition() -> [atom()].

-callback terminate(Reason::term(), State::term()) ->
    ok.

-define(SERVER, ?MODULE).

%% wrap everything with view/epoch tags to make handling these changes
%% easier and less duplicative.
-record(msg,
        {
          view :: non_neg_integer(),
          epoch :: non_neg_integer(),
          sender :: pid(),
          payload :: term()
        }).

-record(client,
        {
          address :: pid(),
          requests = #{} :: #{integer() => request()},
          latest_req = 0 :: non_neg_integer()
        }).
-type client() :: #client{}.

-record(operation,
        {
          num :: non_neg_integer(),
          req_num :: non_neg_integer(),
          view :: non_neg_integer(),
          command :: term(),
          %% not sure that we need this, but also not sure how we
          %% handle the case where we've fallen behind and there has
          %% also been a view change, how do we know when that happend
          %% and what to commit?  pain the the butt to mutate it in
          %% place, tho
          committed = false :: boolean(),
          client :: client()
        }).
-type operation() :: #operation{}.

-record(request,
        {
          req_num :: non_neg_integer(),
          op :: operation(),
          view :: non_neg_integer(),
          reply :: term(),
          from :: tuple()
        }).
-type request() :: #request{}.

-record(replica_state,
        {
          %% replication state
          primary :: boolean(),
          config :: [pid() | atom() | {failed, pid()}], % nodenames

          timeout = make_ref() :: reference(),

          view = 0 :: non_neg_integer(),
          last_normal :: non_neg_integer(),
          status = unconfigured :: unconfigured |
                                   normal |
                                   view_change |
                                   recovering |
                                   transitioning,
          log = ets:new(log,
                        [set,
                         {keypos, 2},
                         private]) :: ets:tid(),
          op =  0 :: non_neg_integer(),
          commit = 0 :: non_neg_integer(),
          client_table = #{} :: #{pid() => client()},
          epoch = 0 :: non_neg_integer(),
          old_config :: [atom()],

          pending_replies = [] :: [],
          nonce :: binary(),

          %% callback module state
          mod :: atom(),
          next_state :: atom(),
          mod_state :: term()
        }).

-define(S, #replica_state).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Mod, ModArgs) ->
    start_link(Mod, ModArgs, true, #{}).

-spec start_link(atom(), [term()], boolean(), #{}) ->
                        {ok, pid()} |
                        {error, Reason::term()}.
start_link(Mod, ModArgs, New, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ModArgs, New, Opts], []).

%% request is should be abstracted to the vrrm_client module, mostly.
-spec request(pid(), term(), vrrm_client:id(), non_neg_integer()) ->
                     {ok, Reply::term()} |
                     {error, Reason::term()}.
request(Primary, Command, Request) ->
    request(Primary, Command, Request, 500).

request(Primary, Command, Request, Timeout) ->
    try
        gen_server:call(Primary, {request, Command, self(), Request}, Timeout)
    catch C:E ->
            lager:warning("request failed with: ~p:~p", [C,E]),
            {error, timeout}
    end.

initial_config(Replica, Config) ->
    gen_server:call(Replica, {initial_config, Config}, infinity).

get_config(Replica) ->
    gen_server:call(Replica, get_config).

reconfigure(Primary, NewConfig, Request) ->
    gen_server:call(Primary, {reconfigure, NewConfig, self(), Request},
                    infinity).

get_mod_state(Replica) ->
    gen_server:call(Replica, get_mod_state).

am_primary(Replica) ->
    gen_server:call(Replica, am_primary).

-record(st, {view :: non_neg_integer(),
             commit :: non_neg_integer(),
             log :: [term()]}).
state_transfer(Replica) ->
    gen_server:call(Replica, state_transfer, infinity).

%% internal messages
-record(prepare, {primary :: pid(), command :: term(),
                  client :: pid(), request :: non_neg_integer(),
                  op :: non_neg_integer(), commit :: non_neg_integer()}).
prepare(Replica, View, Command, Client, Request, Op, Commit, Epoch) ->
    gen_server:cast(Replica,
                    #msg{view = View, epoch = Epoch, sender = self(),
                         payload =
                             #prepare{primary = self(), op = Op,
                                      client = Client, request = Request,
                                      command = Command, commit = Commit}}).

-record(prepare_ok, {op :: non_neg_integer(), sender :: pid(),
                     view :: non_neg_integer()}).
prepare_ok(Replica, View, Op, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #prepare_ok{op = Op,
                                                  %% not sure if this
                                                  %% can be deduplicated
                                                  view = View,
                                                  sender = self()}}).

-record(commit, {commit :: non_neg_integer()}).
commit(Replica, Commit, View, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #commit{commit = Commit}}).

-record(start_view_change, {view :: non_neg_integer(), sender :: pid(),
                            epoch :: non_neg_integer()}).
start_view_change(Replica, View, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #start_view_change{view = View,
                                                         epoch = Epoch,
                                                         sender = self()}}).

-record(do_view_change, {view :: non_neg_integer(), log :: term(),
                         old_view :: non_neg_integer(), op :: non_neg_integer(),
                         commit :: non_neg_integer(), sender :: pid()}).
do_view_change(NewPrimary, View, Log, OldView, Op, Commit, Epoch) ->
    gen_server:cast(NewPrimary, #msg{view = View, epoch = Epoch, sender = self(),
                                     payload =
                                         #do_view_change{view = View,
                                                         log = Log,
                                                         old_view = OldView,
                                                         op = Op,
                                                         commit = Commit,
                                                         sender = self()}}).

-record(start_view, {view :: non_neg_integer(), log :: term(),
                     op :: non_neg_integer(), commit :: non_neg_integer()}).
start_view(Replica, View, Log, Op, Commit, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #start_view{view = View, log = Log,
                                                  op = Op, commit = Commit}}).

out_of_date(Replica, View, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload = out_of_date}).

-record(recovery, {nonce :: binary()}).
recovery(Replica, Nonce, View, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #recovery{nonce = Nonce}}).

-record(recovery_response, {nonce :: binary(), log :: [term()],
                            op :: non_neg_integer(),
                            commit :: non_neg_integer()}).
recovery_response(Replica, View, Nonce, Log, Op, Commit, Epoch) ->
    gen_server:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #recovery_response{nonce = Nonce, op = Op,
                                                         log = Log,
                                                         commit = Commit}}).

-record(start_epoch, {op :: non_neg_integer(), old_config :: [pid()],
                      new_config :: [pid()]}).
start_epoch(Replica, Epoch, Op, OldConfig, NewConfig) ->
    gen_server:cast(Replica, #msg{epoch = Epoch, sender = self(),
                                  payload =
                                      #start_epoch{op = Op,
                                                   old_config = OldConfig,
                                                   new_config = NewConfig}}).

-record(epoch_started, {epoch :: non_neg_integer()}).
epoch_started(Replica, Epoch) ->
    gen_server:cast(Replica, #msg{epoch = Epoch, sender = self(),
                                  payload =
                                      #epoch_started{epoch = Epoch}}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, ModArgs, New, _Opts]) ->
    random:seed(erlang:unique_integer()),
    case New of
        true ->
            %% some ugly duplication here; is there a better way to do
            %% this?
            case Mod:init(ModArgs) of
                {ok, NextState, ModState} ->
                    State = ?S{mod = Mod,
                               next_state = NextState,
                               mod_state = ModState},
                    add_snapshot({NextState, Mod:serialize(ModState)},
                                 0, State?S.log),
                    {ok, State};
                {ok, NextState, ModState, Timeout} ->
                    State = ?S{mod = Mod,
                               next_state = NextState,
                               mod_state = ModState},
                    add_snapshot({NextState, Mod:serialize(ModState)},
                                 0, State?S.log),
                    {ok, State, Timeout}
            end;
        false ->
            %% here we need to create a node without any
            %% configuration or state (since it'll just get thrown
            %% away), then I guess do a reconfiguration
            throw(oh_god)
    end.

%% request
handle_call({request, _Command, _Client, _Request},
            _From,
            ?S{primary = false, config = Config} = State) ->
    Primary = find_primary(Config),
    {reply, {error, not_primary, Primary}, State};
handle_call({request, _Command, _Client, _Request},
            _From,
            ?S{primary = true, status = transitioning} = State) ->
    {reply, reconfiguring, State};
handle_call({request, Command, Client, Request},
            From,
            ?S{log = Log, commit = Commit, timeout = Timeout,
               view = View, op = Op, epoch = Epoch,
               client_table = Table, config = Config} = State) ->
    %% are we primary? ignore (or reply not_primary?) if not
    lager:debug("~p request: command ~p client ~p request ~p",
                [self(), Command, Client, Request]),
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
            ClientRecord = get_client(Client, Table),
            Entry = #operation{num = Op1,
                               req_num = Request,
                               view = View,
                               command = Command,
                               client = Client},
            _ = add_log_entry(Entry, Log),
            RequestRecord = #request{op = Op1,
                                     view = View,
                                     req_num = Request,
                                     from = From},
            Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
            [prepare(Replica, View, Command, Client, Request,
                     Op1, Commit, Epoch)
             || Replica <- Config, Replica /= self(), is_pid(Replica)],
            {noreply, State?S{op = Op1,
                              client_table = Table1,
                              timeout = primary_timeout(Timeout)}}
    end;
handle_call({initial_config, Config}, _From, State) ->
    %% should check that we're in unconfigured+config=undefined
    [Primary | _] = Config,  %% maybe OK?
    AmPrimary = self() =:= Primary,
    lager:debug("~p inserting initial configuration, primary: ~p, ~p",
                [self(), Primary, AmPrimary]),
    Timeout =
        case AmPrimary of
            true ->
                %% trigger initial view change
                [start_view_change(R, 1, 0)
                 || R <- Config],
                %% haxx
                primary_timeout(make_ref());
            false ->
                %% haxx
                replica_timeout(make_ref())
        end,
    {reply, ok, State?S{config = Config,
                        status = normal,
                        primary = AmPrimary,
                        timeout = Timeout}};
handle_call(get_config, _From, ?S{primary = false, config = Config} = State) ->
    Primary = find_primary(Config),
    {reply, {error, not_primary, Primary}, State};
handle_call(get_config, _From, ?S{config = Config} = State) ->
    {reply, Config, State};
handle_call({reconfigure, _NewConfig, _Client, _Request}, _From,
            ?S{primary = false, config = Config} = State) ->
    Primary = find_primary(Config),
    {reply, {error, not_primary, Primary}, State};
handle_call({reconfigure, NewConfig, Client, Request}, From,
            ?S{config = Config, primary = true, status = normal,
               log = Log, client_table = Table, view = View,
               timeout = Timeout,
               commit = Commit, op = Op, epoch = Epoch} = State) ->
    %% validate new config, Epoch == epoch, and Request is not already
    %% processed for this Client (only valid on primary).
    %% if that's all OK: insert a new, special internal operation into
    %% the consensus log & do the normal stuff, then stop accepting
    %% client requests (queue, then deal with them after
    %% start_epoch?). when that completes, the prepare_ok step will
    %% send start_epoch to new nodes

    case recent(Client, Request, Table) of
        {true, Reply} ->
            {reply, Reply, State};
        false ->
            %% TODO: validate new config here

            %% advance the op_num, add request to the log,
            %% update client table cast prepare to other
            %% replicas, return noreply
            Op1 = Op + 1,
            ClientRecord = get_client(Client, Table),
            Entry = #operation{num = Op1,
                               view = View,
                               command = {'$reconfigure', NewConfig},
                               req_num = Request,
                               client = Client},
            _ = add_log_entry(Entry, Log),
            RequestRecord = #request{op = Op1,
                                     view = View,
                                     req_num = Request,
                                     from = From},
            Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
            [prepare(Replica, View, {'$reconfigure', NewConfig}, Client,
                     Request, Op1, Commit, Epoch)
             || Replica <- Config, Replica /= self(), is_pid(Replica)],
            {noreply, State?S{op = Op1, status = transitioning,
                              client_table = Table1,
                              timeout = primary_timeout(Timeout)}}
    end;
handle_call(get_mod_state, _From,
            ?S{next_state = Next, mod_state = Mod} = State) ->
    %% TODO: we need to move to serialize here!
    {reply, {Next, Mod}, State};
handle_call(state_transfer, _From,
            ?S{log = Log, commit = Commit, view = View} = State) ->
    ST = #st{log = ship_log(Log),
             commit = Commit,
             view = View},
    {reply, {ok, ST}, State};
handle_call(am_primary, _From, ?S{primary = Primary} = State) ->
    {reply, Primary, State};
handle_call(_Request, _From, State) ->
    lager:warning("~p unexpected call ~p from ~p~n ~p",
                  [self(), _Request, _From, State]),
    {noreply, State}.

handle_cast(#msg{view = View, epoch = Epoch, sender = Sender,
                 payload = Payload} = _Msg,
            ?S{view = LocalView, epoch = LocalEpoch} = State)
  when LocalEpoch > Epoch; LocalView > View ->
    %% ignore these, as they're invalid under the current protocol,
    lager:debug("~p discarding message from ~p with old view or epoch: ~p",
                [self(), Sender, _Msg]),
    %% this is a horrible thing to work around some potential races, I
    %% need to figure out something better eventually.
    if is_record(Payload, do_view_change) andalso
       View =:= LocalView - 1 ->
            ok;
       is_record(Payload, prepare_ok) andalso
       Epoch =:= LocalEpoch - 1 ->
            ok;
       %% otherwise we should let the other node know it's out of date.
       true ->
            out_of_date(Sender, LocalView, LocalEpoch)
    end,
    {noreply, State};
handle_cast(#msg{payload = Payload} = _Msg, ?S{status=recovering} = State)
  when not is_record(Payload, recovery) andalso
       not is_record(Payload, recovery_response) ->
    %% ignore other protocols when we're in recovery mode.
    lager:debug("~p discarding non-recovery message: ~p", [self(), _Msg]),
    {noreply, State};
handle_cast(#msg{payload = #prepare{primary = Primary, command = Command,
                                    client = Client, request = Request,
                                    op = Op, commit = Commit},
                 view = View} = Msg,
            ?S{timeout = Timeout} = State0) ->
    lager:debug("~p prepare: command ~p", [self(), Msg]),
    %% is log complete? consider state transfer if not
    %% if CommitNum is higher than commit_num, do some upcalls
    State = maybe_catchup(Commit, State0),
    Table = State?S.client_table,
    ClientRecord = get_client(Client, Table),
    RequestRecord = #request{op = Op,
                             view = View,
                             req_num = Request},
    Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
    %% increment op number, append operation to log, update client table(?)
    Entry = #operation{num = Op,
                       client = Client,
                       view = View,
                       command = Command,
                       req_num = Request},
    _ = add_log_entry(Entry, State?S.log),
    %% reply with prepare_ok
    prepare_ok(Primary, View, Op, Msg#msg.epoch),
    %% reset primary failure timeout
    {noreply, State?S{op = Op,
                      client_table = Table1,
                      timeout = replica_timeout(Timeout)}};
handle_cast(#msg{payload = #prepare_ok{op = Op}},
            ?S{commit = Commit} = State)
  when Commit >= Op ->
    %% ignore these, we've already processed them.
    {noreply, State};
handle_cast(#msg{payload = #prepare_ok{op = Op} = Msg},
            ?S{mod = Mod, next_state = NextState, mod_state = ModState,
               client_table = Table, log = Log, view = View,
               pending_replies = Pending, config = Config,
               epoch = Epoch} = State) ->
    %% do we have f replies? if no, wait for more (or timeout)
    F = f(State?S.config),
    OKs = scan_pend(prepare_ok, {Op, View}, Pending),
    lager:debug("~p prepare_ok: ~p, ~p N ~p F ~p ~p",
                [self(), Msg, Pending, OKs, F, {View, Op}]),
    case length(OKs ++ [Msg]) of
        N when N >= F ->
            lager:debug("got enough, replying"),
            %% update commit_num to OpNum, do Mod upcall, send {reply,
            %% v, s, x} set commit message timeout.
            #operation{command = Command,
                       req_num = Request,
                       client = Client} =
                         get_log_entry(Op, Log),
            case Command of
                {'$reconfigure', NewConfig} ->
                    lager:debug("~p reconfigure succeded", [self()]),
                    NewNodes = NewConfig -- Config,
                    Epoch1 = Epoch + 1,
                    [start_epoch(Replica, Epoch1, Op, Config, NewConfig)
                     || Replica <- NewNodes,
                        is_pid(Replica),
                        Replica /= self()],
                    %% send a commit to the remaining nodes so we
                    %% don't have to wait for one
                    [commit(Replica, Op, 0, Epoch1)
                     || Replica <- Config -- NewNodes,
                        Replica /= self(),
                        is_pid(Replica)],
                    Reply = ok,
                    View1 = 0,
                    NextState1 = NextState,
                    ModState1 = ModState;
                _ ->
                    %% this needs to grow proper handling at some point
                    {reply, Reply, NextState1, ModState1} =
                        Mod:NextState(Command, ModState),
                    View1 = View,
                    Epoch1 = Epoch
            end,
            _ = update_log_commit_state(Op, Log),
            Interval = vrrm:config(snapshot_op_interval),
            _ = maybe_compact(Op, Interval, Mod, {NextState1, ModState1}, Log),
            CliRec = get_client(Client, Table),
            ReqRec = get_request(CliRec, Request),
            lager:debug("cr = ~p req = ~p", [CliRec, ReqRec]),
            case ReqRec#request.from of
                undefined ->
                    %% from isn't portable between nodes?? client will
                    %% need to retry
                    ok;
                From ->
                    gen_server:reply(From, {ok, Reply, Epoch})
            end,
            _ = add_reply(Client, ReqRec#request.req_num, Reply, Table),
            Pending1 = clean_pend(prepare_ok, {Op, View}, Pending),
            %% do we need to check if commit == Op - 1?
            {noreply, State?S{commit = Op, epoch = Epoch1, view = View1,
                              mod_state = ModState1, next_state = NextState1,
                              pending_replies = Pending1}};
        _ ->
            %% wait for more
            %% addendum: this is bad FIXME, need to dedupe!
            Pending1 = [Msg|Pending],
            {noreply, State?S{pending_replies = Pending1}}
    end;
handle_cast(#msg{payload=#commit{commit = Commit},
                 epoch = Epoch},
            ?S{timeout = Timeout} = State0) ->
    %% if Commit > commit_num, do upcalls, doing state transfer if
    %% needed.
    CurrentEpoch = State0?S.epoch,
    case Epoch of
        CurrentEpoch ->
            State = maybe_catchup(Commit, State0);
        _ ->
            State = maybe_transition(Commit, Epoch, State0)
    end,
    %% reset primary failure timeout, which should be configurable
    {noreply, State?S{timeout = replica_timeout(Timeout)}};
handle_cast(#msg{payload=#start_view_change{}, view=View, epoch=Epoch,
                 sender = Sender},
            ?S{op = Op, view = OldView,
               config = Config, log = Log0, status = Status,
               commit = Commit, timeout = Timeout} = State) ->
    lager:debug("~p start_view_change: ~p", [self(), View]),
    %% calculate new primary here
    ExistingPrimary = find_primary(Config),
    {Primary, Config1} =
        case Sender of
            ExistingPrimary ->
                %% primary is requesting a new view for administrative
                %% reasons rather than failure
                {ExistingPrimary, Config};
            _ ->
                 fail_primary(Config)
        end,
    Log = ship_log(Log0),
    lager:info("~p sending do view change ~p -> ~p to ~p",
               [self(), OldView, View, Primary]),
    do_view_change(Primary, View, Log, OldView, Op, Commit, Epoch),
    %% change status to view_change if not already there, and note last_normal
    {Status1, LastNormal1} =
        case Status of
            normal ->
                {view_change, OldView};
            _ ->
                {Status, State?S.last_normal}
        end,
    %% reset interval change to avoid storm?
    {noreply, State?S{status = Status1, last_normal = LastNormal1,
                      config = Config1,
                      timeout = replica_timeout(Timeout)}};
handle_cast(#msg{payload = #do_view_change{view = View}},
            ?S{view = View} = State) ->
    %% these are late, drop them.
    {noreply, State};
handle_cast(#msg{payload = #do_view_change{view = View} = Msg},
            ?S{pending_replies = Pending, epoch = Epoch,
               log = Log, config = Config, commit = Commit} = State) ->
    %% have we gotten f + 1 (inclusive of self)? if no, wait
    F = f(State?S.config),
    OKs0 = scan_pend(do_view_change, View, Pending),
    lager:info("~p do_view_change: ~p N ~p F ~p",
                [self(), length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    HasSelf = has_self(OKs, self()),
    case length(OKs) of
        N when N >= F + 1 andalso HasSelf ->
            %% if yes (and all new ViewNums agree) set op num to largest in
            %% result-set, select new log from result set member with the
            %% largest op_num.  set commit number to largest number in the
            %% result set, return status to normal, and send start_view to all
            %% other replicas (maybe compacting log first)
            lager:info("OKs: ~p", [OKs]),
            HighestOp = get_highest_op(OKs),
            HighOp = HighestOp#do_view_change.op,
            HighLog = HighestOp#do_view_change.log,
            _ = sync_log(Commit, Log, HighOp, HighLog),
            HighCommit = get_highest_commit(OKs),
            [start_view(Replica, View, HighLog, HighOp, HighCommit, Epoch)
             || Replica <- Config, is_pid(Replica)],
            lager:debug("~p view change successful", [self()]),
            Pending1 = clean_pend(do_view_change, View, Pending),
            {noreply, State?S{status = normal, op = HighOp, view = View,
                              pending_replies = Pending1,
                              primary = true, commit = HighCommit}};
                              %% next_state = NextState, mod_state = ModState}};
        _ ->
            Pending1 = [Msg | Pending],
            {noreply, State?S{pending_replies = Pending1}}
    end;
handle_cast(#msg{payload=#start_view{view = View, log = Log, op = Op,
                                     commit = Commit},
                 epoch = Epoch},
            ?S{log = LocalLog, commit = LocalCommit,
               primary = AmPrimary,
               config = Config, mod_state = ModState,
               old_config = OldConfig, timeout = Timeout,
               mod = Mod, next_state = NextState} = State) ->
    %% replace log with Log, set op_num to highest op in the log, set
    %% view_num to View, if there are uncommitted operations in the
    %% log, send prepare_ok messages to the primary for them, commit
    %% any known commited and update commit number.
    Primary = find_primary(Config),
    Commits = sync_log(LocalCommit, LocalLog, Op, Log),
    {NextState1, ModState1, {OldConfig1, Config1}}
        = do_upcalls(Mod, NextState, ModState, LocalLog,
                     {OldConfig, Config}, Commits),
    [prepare_ok(Primary, View, PrepOp, Epoch)
     || PrepOp <- lists:seq(Commit + 1, Op)],
    {noreply, State?S{op = Op, view = View, status = normal,
                      config = Config1, old_config = OldConfig1,
                      mod_state = ModState1, next_state = NextState1,
                      timeout = select_timeout(AmPrimary, Timeout)}};
handle_cast(#msg{payload=out_of_date, view = View, epoch = Epoch},
            ?S{config = Config} = State) ->
    %% moderately sure that epoch handling here is wrong.  how to test?
    lager:debug("~p out_of_date", [self()]),
    Nonce = crypto:rand_bytes(16),
    [recovery(Replica, Nonce, View, Epoch)
     || Replica <- Config, is_pid(Replica), Replica /= self()],
    {noreply, State?S{view = View, epoch = Epoch,
                      status = recovering, nonce = Nonce}};
handle_cast(#msg{payload=#recovery{nonce = Nonce}, sender = Sender},
            ?S{primary = false, view = View, epoch = Epoch,
               config = Config} = State) ->
    %% if status is normal, send recovery_response
    lager:debug("~p recovery, non-primary", [self()]),
    Config1 = unfail_replica(Sender, Config),
    recovery_response(Sender, View, Nonce, undefined,
                      undefined, undefined, Epoch),
    {noreply, State?S{config = Config1}};
handle_cast(#msg{payload=#recovery{nonce = Nonce}, sender = Sender},
            ?S{primary = true, view = View, log = Log0,
               op = Op, commit = Commit, epoch = Epoch,
               config = Config} = State) ->
    %% if primary, include additional information
    lager:debug("~p recovery, primary", [self()]),
    Config1 = unfail_replica(Sender, Config),
    Log = ship_log(Log0),
    recovery_response(Sender, View, Nonce, Log, Op, Commit, Epoch),
    {noreply, State?S{config = Config1}};
handle_cast(#msg{payload=#recovery_response{nonce = Nonce} = Msg,
                 view = View, sender = _Sender, epoch = Epoch},
            ?S{log = LocalLog, commit = LocalCommit, mod = Mod,
               config = Config, old_config = OldConfig,
               next_state = NextState, mod_state = ModState,
               pending_replies = Pending} = State) ->
    lager:debug("~p recovery_response ~p", [self(), _Sender]),
    %% have we got f + 1 incuding the primary response? if no wait
    F = f(State?S.config),
    OKs0 = scan_pend(recovery_response, Nonce, Pending),
    lager:debug("~p recovery_response: ~p, ~p N ~p F ~p",
                [self(), Msg, length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    {Primary, HasPrimary} = has_primary(OKs),
    case length(OKs) of
        N when N >= F + 1 andalso HasPrimary ->
            #recovery_response{log = Log, op = Op, commit = Commit} = Primary,
            %% if yes, use primary values, set to normal, restart participation
            _ = trunc_uncommitted(LocalLog, LocalCommit),
            Commits = sync_log(LocalCommit, LocalLog, Op, Log),
            {NextState1, ModState1, {OldConfig1, Config2}} =
                do_upcalls(Mod, NextState, ModState, LocalLog,
                           {OldConfig, Config}, Commits),
            Config1 = unfail_replica(self(), Config2),
            Pending1 = clean_pend(recovery_response, Nonce, Pending),
            case Config1 =/= Config andalso
                not lists:member(self(), Config1) of
                true ->
                    %% the epoch has changed at some point in our log
                    %% replication, we need to check that we still
                    %% exist in the new system.
                    {stop, normal,
                     State?S{next_state = NextState1,
                             mod_state = ModState1,
                             pending_replies = Pending1}};
                _ ->
                    %% if we're the recovering primary of this quorum, we need
                    %% to initiate another view change
                    case State?S.primary of
                        true ->
                            [start_view_change(R, View + 1, Epoch)
                             || R <- State?S.config];
                        _ -> ok
                    end,
                    lager:debug("~p recover successful", [self()]),
                    {noreply, State?S{status = normal, next_state = NextState1,
                                      mod_state = ModState1, op = Op,
                                      config = Config1, old_config = OldConfig1,
                                      commit = Commit,
                                      pending_replies = Pending1}}
            end;
        _ ->
            {noreply, State?S{pending_replies = [Msg|Pending]}}
    end;
handle_cast(#msg{epoch = Epoch, sender = Sender,
                 payload = #start_epoch{}},
            ?S{status = normal, epoch = Epoch} = State) ->
    %% if we're normal and in Epoch, reply with epoch_started
    epoch_started(Sender, Epoch),
    {noreply, State};
handle_cast(#msg{payload=#start_epoch{op = Op, old_config = OldConfig,
                                      new_config = NewConfig},
                 epoch = Epoch},
            ?S{log = Log, mod = Mod} = State) ->
    %% this state is for non-prewarmed new nodes in a configuration
    %% record Epoch, Op, and configs, set view number = 0, and state
    %% to transitioning^W normal, since we block here forever trying
    %% to catch up..

    %% pick a node at random from the nodes that will be staying
    Source = pick(NewConfig -- (NewConfig -- OldConfig)),
    {ok, ST } = state_transfer(Source),
    lager:debug("~p got transfer ~p from ~p", [self(), ST, Source]),
    _ = install_log(Log, ST#st.log),
    {SnapOp, {NextState, ModState0}} =
        get_snapshot(Log),
    ModState = Mod:deserialize(ModState0),
    case SnapOp of
        Op ->
            ModState1 = ModState,
            NextState1 = NextState;
        _ ->
            %% we use ignore here because we cannot have two
            %% concurrent epoch changes in flight
            {NextState1, ModState1, ignore}
                = do_upcalls(Mod, NextState, ModState, Log,
                             ignore, SnapOp + 1, ST#st.commit)
    end,

    %% send leaving nodes epoch_started
    [epoch_started(Replica, Epoch)
     || Replica <- OldConfig -- NewConfig, is_pid(Replica)],
    {noreply, State?S{epoch = Epoch, status = normal,
                      old_config = OldConfig, config = NewConfig,
                      commit = ST#st.commit, op = Op,
                      view = ST#st.view, next_state = NextState1,
                      mod_state = ModState1}};
handle_cast(#msg{payload = #epoch_started{} = Msg, epoch = Epoch},
            ?S{pending_replies = Pending} = State) ->
    %% if we've gotten f + 1 of these, we die.
    %% TODO: if we don't get enough in time, we re-send start_epoch to
    %% new nodes
    F = f(State?S.config),
    OKs0 = scan_pend(epoch_started, Epoch, Pending),
    lager:debug("~p epoch_started : ~p, ~p N ~p F ~p",
               [self(), Msg, length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    case length(OKs) of
        N when N >= F + 1 ->
            {stop, normal, State};
        _ ->
            {noreply, State?S{pending_replies = [Msg|Pending]}}
    end;
handle_cast(_Msg, State) ->
    lager:warning("~p unexpected cast ~p", [self(), _Msg]),
    {noreply, State}.

handle_info(commit_timeout,
            ?S{primary = true, view = View, epoch = Epoch,
               commit = Commit, config = Config,
               timeout = Timeout} = State) ->
    %% send commit message to all replicas
    [commit(R, Commit, View, Epoch)
     || R <- Config, is_pid(R), R /= self()],
    {noreply, State?S{timeout = primary_timeout(Timeout)}};
%% need to monitor primary and catch monitor failures here
handle_info(primary_timeout,
            ?S{primary = false, view = View,
               timeout = Timeout, epoch = Epoch} = State) ->
    %% initiate the view change by incrementing the view number and
    %% sending a start_view change message to all reachable replicas.
    lager:debug("~p detected primary timeout, starting view change",
               [self()]),
    [_|Config] = State?S.config,
    [start_view_change(R, View + 1, Epoch)
     || R <- Config, is_pid(R)],
    {noreply, State?S{view = View,
                      timeout = replica_timeout(Timeout)}};
handle_info(timeout, ?S{mod = Mod, next_state = NextState,
                        mod_state = ModState} = State) ->
    case Mod:NextState(timeout, ModState) of
        {next_state, NextState1, ModState1} ->
            {noreply, State?S{next_state = NextState1,
                              mod_state = ModState1}};
        {stop, Reason, ModState1} ->
            {stop, Reason, State?S{mod_state = ModState1}}
    end;
handle_info(Msg, ?S{mod = Mod, mod_state = ModState} = State) ->
    try Mod:handle_info(Msg, ModState) of
        {next_state, NextState1, ModState1} ->
            {noreply, State?S{next_state = NextState1,
                              mod_state = ModState1}};
        {stop, Reason, ModState1} ->
            {stop, Reason, State?S{mod_state = ModState1}}
    catch error:function_clause ->
            lager:warning("~p unexpected message ~p", [self(), Msg]),
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_client(Pid, Table) ->
    case maps:find(Pid, Table) of
        {ok, Client} ->
            Client;
        _ ->
            #client{address = Pid}
    end.

recent(Client, Request, Table) ->
    case maps:find(Client, Table) of
        [Record] ->
            in_requests(Request, Record#client.requests);
        _ ->
            false
    end.

in_requests(Request, Requests) ->
    case maps:find(Request, Requests) of
        {ok, #request{op = Op,
                      reply = Reply}} ->
            case Op of
                #operation{committed = true} ->
                    {true, Reply};
                _ ->
                    false
            end;
        _ ->
            false
    end.

get_request(#client{requests = Requests} = _C, Request) ->
    lager:debug("~p get_request c ~p o ~p",
                [self(), _C, Request]),
    case maps:find(Request, Requests) of
        {ok, #request{} = RR} ->
            RR;
        _E ->
            error({from, _E})
    end.

add_request(Pid, #client{requests = Requests} = Client,
            #request{req_num = RequestNum} = Request, Table) ->
    maps:put(Pid, Client#client{requests = maps:put(RequestNum,
                                                    Request,
                                                    Requests)},
             Table).

add_reply(Client, RequestNum, Reply, Table) ->
    CR = maps:get(Client, Table),
    #client{requests = Requests} = CR,
    Request = maps:get(RequestNum, Requests),
    Requests1 = maps:put(RequestNum,
                         Request#request{reply = Reply},
                         Requests),
    maps:put(Client, CR#client{requests = Requests1}, Table).

add_log_entry(Entry, Log) ->
    lager:debug("adding log entry ~p", [Entry]),
    ets:insert(Log, Entry).

get_log_entry(Op, Log) ->
    lager:debug("~p looking up log entry ~p", [self(), Op]),
    case ets:lookup(Log, Op) of
        [OpRecord] ->
            OpRecord;
        _Huh -> error({noes, _Huh})
    end.

%% we're skipping the extra state here and will need to handle
%% catching up later I guess
maybe_transition(Commit, Epoch, State) ->
    maybe_catchup(Commit, State?S{epoch = Epoch, view = 0}).

maybe_catchup(Commit, ?S{commit = LocalCommit} = State)
  when LocalCommit >= Commit ->
    State;
maybe_catchup(Commit, ?S{mod = Mod, next_state = NextState,
                         mod_state = ModState, log = Log,
                         config = Config, old_config = OldConfig,
                         commit = LocalCommit} = State) ->
    {NextState1, ModState1, {OldConfig1, Config1}}
        = do_upcalls(Mod, NextState, ModState, Log,
                     {OldConfig, Config}, LocalCommit + 1, Commit),
    State?S{next_state = NextState1, mod_state = ModState1,
            config = Config1, old_config = OldConfig1,
            commit = Commit}.

do_upcalls(Mod, NextState, ModState, Log, Configs, Start, Stop) ->
    do_upcalls(Mod, NextState, ModState, Log, Configs, lists:seq(Start, Stop)).

do_upcalls(Mod, NextState, ModState, Log, Configs, Commits) ->
    lists:foldl(fun(Op, {NState, MState, Cfgs}) ->
                        Entry = get_log_entry(Op, Log),
                        case Entry#operation.command of
                            %% ugh this is ugly
                            {'$reconfigure', NewConfig} ->
                                Cfgs1 =
                                    case Cfgs of
                                        ignore -> ignore;
                                        {_, Config} ->
                                            {Config, NewConfig}
                                    end,
                                {NState, MState, Cfgs1};
                            Cmd ->
                                case Mod:NState(Cmd, MState) of
                                    {reply, _, NState1, MState1} ->
                                        %% should be updating client table
                                        %% here too?
                                        update_log_commit_state(Op, Log),
                                        {NState1, MState1, Cfgs};
                                    {next_state, NState1, MState1} ->
                                        update_log_commit_state(Op, Log),
                                        {NState1, MState1, Cfgs}
                                end
                        end
                end,
                {NextState, ModState, Configs},
                Commits).

f(Config) ->
    L = length(Config),
    (L-1) div 2.

%% manually unroll this because the compiler is not smart
scan_pend(prepare_ok, {Op, View}, Pending) ->
    [R || #prepare_ok{view = V, op = O} = R <- Pending,
          O == Op andalso V == View];
scan_pend(do_view_change, View, Pending) ->
    [R || #do_view_change{view = V} = R <- Pending,
          V == View];
scan_pend(recovery_response, Nonce, Pending) ->
    [R || #recovery_response{nonce = N} = R <- Pending,
          N =:= Nonce];
scan_pend(epoch_started, Epoch, Pending) ->
    [R || #epoch_started{epoch = E} = R <- Pending,
          E =:= Epoch];
scan_pend(_Msg, _Context, _Pending) ->
    error(unimplemented).

%% also need to GC this eventually?
clean_pend(prepare_ok, {Op, View}, Pending) ->
    lists:filter(fun(#prepare_ok{op=O, view=V})
                       when O == Op, V == View->
                         false;
                    (_) ->
                         true
                 end, Pending);
clean_pend(do_view_change, View, Pending) ->
    lists:filter(fun(#do_view_change{view=V})
                       when V == View ->
                         false;
                    (_) ->
                         true
                 end, Pending);
clean_pend(recovery_response, Nonce, Pending) ->
    lists:filter(fun(#recovery_response{nonce = N})
                       when N == Nonce ->
                         false;
                    (_) ->
                         true
                 end, Pending);
clean_pend(_Msg, _Context, _Pending) ->
    error(unimplemented).

add_snapshot(Snap, Op, Log) ->
    %% this is the worst
    ets:insert(Log, {Op, snapshot, Snap}).

get_snapshot(Log) ->
    [{Op, _, Snap}] = ets:lookup(Log, snapshot),
    {Op, Snap}.

update_log_commit_state(Op, Log) ->
    [Operation] = ets:lookup(Log, Op),
    lager:debug("setting ~p ~p to committed", [Op, Operation]),
    ets:insert(Log, Operation#operation{committed = true}).

ship_log(Log) ->
    ets:foldl(fun(X, Acc) -> [X|Acc] end, [], Log).

trunc_uncommitted(Log, Commit) ->
    %% this is an operation, rather than the snapshot hack
    ets:select_delete(Log, [{{'_','$1','_','_','_','_','_'},
                             [],[{'>','$1',Commit}]}]).

sync_log(OldCommit, OldLog, NewOpNum, NewLog) ->
    %% get a list of ops
    Ops0 =
        [case ets:lookup(OldLog, Op) of
             %% we've never heard of this one, insert
             [] ->
                 NewOp = lists:keyfind(Op, 2, NewLog),
                 ets:insert(OldLog, NewOp),
                 %% return the op because we need to commit it
                 %% may need to tag
                 Op;
             [#operation{committed = true} = OldOp] ->
                 NewOp = lists:keyfind(Op, 2, NewLog),
                 %% assert that ops are the same for safety
                 NewCmd = NewOp#operation.command,
                 OldCmd = OldOp#operation.command,
                 NewCmd = OldCmd,
                 [];
             [#operation{}]->
                 Op
         end
         || Op <- lists:seq(OldCommit + 1, NewOpNum)],
    lists:flatten(Ops0).

%% safe to assume this is blank?
install_log(Log, NewLog) ->
    _ = [ets:insert(Log, X) || X <- NewLog],
    ok.

maybe_compact(Op, Interval, _Mod, _Snap, _Log) when Op rem Interval /= 0 ->
    ok;
maybe_compact(Op, _, Mod, {NS, MS0}, Log) ->
    Snap = {NS, Mod:serialize(MS0)},
    %% make current app state the snapshot
    _ = add_snapshot(Op, Snap, Log),
    %% remove all log entries before Op
    ets:select_delete(Log, [{{'_', '$1', '_', '_', '_', '_', '_'},
                             [],
                             [{'=<','$1',Op}]}]).

get_highest_op(Msgs) ->
    lager:debug("get highest ~p", [Msgs]),
    get_highest_op(Msgs, 0, undefined).

get_highest_op([], _HighOp, HighMsg) ->
    HighMsg;
get_highest_op([H|T], HighOp, HighMsg) ->
    case H#do_view_change.op >= HighOp of
        true ->
            get_highest_op(T, H#do_view_change.op, H);
        _ ->
            get_highest_op(T, HighOp, HighMsg)
    end.

get_highest_commit(Msgs) ->
    get_highest_commit(Msgs, 0, undefined).

get_highest_commit([], HighCommit, _HighMsg) ->
    HighCommit;
get_highest_commit([H|T], HighCommit, HighMsg) ->
    case H#do_view_change.commit > HighCommit of
        true ->
            get_highest_commit(T, H#do_view_change.commit, H);
        _ ->
            get_highest_commit(T, HighCommit, HighMsg)
    end.

has_self([#do_view_change{sender = Self}|_], Self) ->
    true;
has_self([_|T], Self) ->
    has_self(T, Self);
has_self([], _) ->
    false.

has_primary([]) ->
    {meh, false};
has_primary([#recovery_response{log=Log}=P|_T]) when Log =/= undefined->
    {P, true};
has_primary([_|T]) ->
    has_primary(T).


fail_primary(Config) ->
    fail_primary(Config, first, []).

fail_primary([], _, Acc) ->
    Config = lists:reverse(Acc),
    {find_primary(Config), Config};
fail_primary([{failed, _} = H|T], first, Acc) ->
    fail_primary(T, first, [H|Acc]);
fail_primary([H|T], first, Acc) ->
    lager:info("~p marking ~p failed", [self(), H]),
    fail_primary(T, rest, [{failed, H}|Acc]);
fail_primary([H|T], rest, Acc) ->
    fail_primary(T, rest, [H|Acc]).

unfail_replica(Replica, Config) ->
    unfail_replica(Replica, Config, []).

unfail_replica(_Replica, [], Acc) ->
    lists:reverse(Acc);
unfail_replica(Replica, [{failed, Replica}|T], Acc) ->
    unfail_replica(Replica, T, [Replica|Acc]);
unfail_replica(Replica, [H|T], Acc) ->
    unfail_replica(Replica, T, [H|Acc]).

find_primary([{failed, _}|T]) ->
    find_primary(T);
find_primary([H|_]) ->
    H.

select_timeout(true, TRef) ->
    primary_timeout(TRef);
select_timeout(false, TRef) ->
    replica_timeout(TRef).

primary_timeout(TRef) ->
    erlang:cancel_timer(TRef, []),
    TO = vrrm:config(idle_commit_interval),
    erlang:send_after(TO, self(), commit_timeout).

replica_timeout(TRef) ->
    erlang:cancel_timer(TRef, []),
    TO  = jitter(20, vrrm:config(primary_failure_interval)),
    erlang:send_after(TO, self(), primary_timeout).

jitter(Pct, Num) ->
    M = 100 + (random:uniform(Pct * 2) - Pct),
    (M * Num) div 100.

pick(Lst) ->
    lists:nth(random:uniform(length(Lst)), Lst).
