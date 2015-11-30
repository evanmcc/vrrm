-module(vrrm_replica).

-behaviour(gen_server).

%% API
-export([
         start_link/4
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

         %% reconfiguration/2,
         %% start_epoch/2,
         %% epoch_started/2,

         %% %% sync
         %% get_state/2,
         %% get_op/2, get_op/3,
         request/3, request/4,
         initial_config/2 %%,
         %% reconfigure/2,
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
          config :: [atom()], % nodenames
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
          client_table = ets:new(table,
                                 [set,
                                  {keypos, 2},
                                  private]) :: ets:tid(),
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
            lager:info("req to ~p:~p", [C,E]),
            {error, timeout}
    end.

initial_config(Replica, Config) ->
    gen_server:call(Replica, {initial_config, Config}, infinity).

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
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, ModArgs, New, _Opts]) ->
    random:seed(erlang:unique_integer()),
    case New of
        true ->
            {ok, NextState, ModState} = Mod:init(ModArgs),
            State =
                ?S{mod = Mod,
                   next_state = NextState,
                   mod_state = ModState
                  },
            add_snapshot({NextState, ModState}, 0, State?S.log),
            {ok, State};
        false ->
            %% here we need to create a node without any
            %% configuration or state (since it'll just get thrown
            %% away), then I guess do a reconfiguration
            throw(oh_god)
    end.

%% request
handle_call({request, Command, Client, Request},
            From,
            ?S{primary = Primary, log = Log,
               commit = Commit, %% replica = Self,
               view = View, op = Op, epoch = Epoch,
               client_table = Table, config = Config} = State) ->
    %% are we primary? ignore (or reply not_primary?) if not
    lager:info("~p request: command ~p client ~p request ~p",
               [self(), Command, Client, Request]),
    case Primary of
        false ->
            {reply, not_primary, State};
        true ->
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
                                       view = View,
                                       command = Command,
                                       client = Client},
                    _ = add_log_entry(Entry, Log),
                    RequestRecord = #request{op = Op1,
                                             view = View,
                                             req_num = Request,
                                             from = From},
                    _ = add_request(ClientRecord, RequestRecord, Table),
                    [prepare(Replica, View, Command, Client, Request,
                             Op1, Commit, Epoch)
                     || Replica <- Config, Replica /= self(), is_pid(Replica)],
                    {noreply, State?S{op = Op1}, primary_timeout()}
            end
    end;
handle_call({initial_config, Config}, _From, State) ->
    %% should check that we're in unconfigured+config=undefined
    [Primary | _] = Config,  %% maybe OK?
    AmPrimary = self() =:= Primary,
    lager:info("~p inserting initial configuration, primary: ~p, ~p",
               [self(), Primary, AmPrimary]),
    Timeout =
        case AmPrimary of
            true ->
                primary_timeout();
            false ->
                replica_timeout()
        end,
    {reply, ok, State?S{config = Config,
                        status=normal,
                        primary=AmPrimary},
     Timeout};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    {noreply, State}.

handle_cast(#msg{view = View, epoch = Epoch, sender = Sender} = Msg,
            ?S{view = LocalView, epoch = LocalEpoch} = State)
  when LocalEpoch > Epoch; LocalView > View ->
    %% ignore these, as they're invalid under the current protocol
    lager:info("~p discarding message with old view or epoch: ~p",
               [self(), Msg]),
    out_of_date(Sender, LocalView, LocalEpoch),
    {noreply, State};
handle_cast(#msg{payload = Payload} = Msg, ?S{status=recovering} = State)
  when not is_record(Payload, recovery) andalso
       not is_record(Payload, recovery_response) ->
    %% ignore other protocols when we're in recovery mode.
    lager:info("~p discarding non-recovery message: ~p", [self(), Msg]),
    {noreply, State};
handle_cast(#msg{payload = #prepare{primary = Primary, command = Command,
                                    client = Client, request = Request,
                                    op = Op, commit = Commit},
                 view = View} = Msg,
            State0) ->
    lager:info("~p prepare: command ~p", [self(), Msg]),
    %% is log complete? consider state transfer if not
    %% if CommitNum is higher than commit_num, do some upcalls
    State = maybe_catchup(Commit, State0),
    Table = State?S.client_table,
    ClientRecord = get_client(Client, Table),
    RequestRecord = #request{op = Op,
                             view = View,
                             req_num = Request},
    _ = add_request(ClientRecord, RequestRecord, Table),
    %% increment op number, append operation to log, update client table(?)
    Entry = #operation{num = Op,
                       client = Client,
                       view = View,
                       command = Command},
    _ = add_log_entry(Entry, State?S.log),
    %% reply with prepare_ok
    prepare_ok(Primary, View, Op, Msg#msg.epoch),
    %% reset primary failure timeout
    {noreply, State?S{op = Op}, replica_timeout()};
handle_cast(#msg{payload = #prepare_ok{op = Op}},
            ?S{commit = Commit} = State)
  when Commit > Op ->
    %% ignore these, we've already processed them.
    {noreply, State};
handle_cast(#msg{payload = #prepare_ok{op = Op} = Msg},
            ?S{mod = Mod, next_state = NextState, mod_state = ModState,
               client_table = Table, log = Log, view = View,
               pending_replies = Pending} = State) ->
    %% do we have f replies? if no, wait for more (or timeout)
    F = f(State?S.config),
    OKs = scan_pend(prepare_ok, Op, View, Pending),
    lager:info("~p prepare_ok: ~p, ~p N ~p F ~p ~p",
               [self(), Msg, Pending, OKs, F, {View, Op}]),
    case length(OKs ++ [Msg]) of
        N when N >= F ->
            lager:debug("got enough, replying"),
            %% update commit_num to OpNum, do Mod upcall, send {reply,
            %% v, s, x} set commit message timeout.
            #operation{command = Command, client = Client} =
                         get_log_entry(Op, Log),
            {reply, Reply, NextState1, ModState1} =
                Mod:NextState(Command, ModState),
            update_log_commit_state(Op, Log),
            CliRec = get_client(Client, Table),
            ReqRec = get_request(CliRec, Op, View),
            lager:info("cr = ~p req = ~p", [CliRec, ReqRec]),
            case ReqRec#request.from of
                undefined ->
                    %% from isn't portable between nodes?? client will
                    %% need to retry
                    ok;
                From ->
                    gen_server:reply(From, {ok, Reply})
            end,
            _ = add_reply(Client, ReqRec#request.req_num, Reply, Table),
            Pending1 = clean_pend(prepare_ok, Op, View, Pending),
            %% do we need to check if commit == Op - 1?
            {noreply, State?S{commit = Op,
                              mod_state = ModState1, next_state = NextState1,
                              pending_replies = Pending1}};
        _ ->
            %% wait for more
            %% addendum: this is bad FIXME, need to dedupe!
            Pending1 = [Msg|Pending],
            {noreply, State?S{pending_replies = Pending1}}
    end;
handle_cast(#msg{payload=#commit{commit = Commit}},
            State0) ->
    %% if Commit > commit_num, do upcalls, doing state transfer if
    %% needed.
    State = maybe_catchup(Commit, State0),
    %% reset primary failure timeout, which should be configurable
    {noreply, State, replica_timeout()};
handle_cast(#msg{payload=#start_view_change{}, view=View, epoch=Epoch},
            ?S{op = Op, view = OldView,
               config = Config, log = Log0, status = Status,
               commit = Commit} = State) ->
    lager:info("~p start_view_change: ~p", [self(), View]),
    %% calculate new primary here
    {Primary, Config1} = fail_primary(Config),
    Log = ship_log(Log0),
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
                      view = View, config = Config1}, replica_timeout()};
handle_cast(#msg{payload = #do_view_change{view = View} = Msg},
            ?S{pending_replies = Pending, epoch = Epoch,
               log = Log, config = Config, commit = Commit} = State) ->
    %% have we gotten f + 1 (inclusive of self)? if no, wait
    F = f(State?S.config),
    OKs0 = scan_pend(do_view_change, ignore, View, Pending),
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
            %% lager:info("OKs: ~p", [OKs]),
            HighestOp = get_highest_op(OKs),
            HighOp = HighestOp#do_view_change.op,
            HighLog = HighestOp#do_view_change.log,
            _ = sync_log(Commit, Log, HighOp, HighLog),
            HighCommit = get_highest_commit(OKs),
            [start_view(Replica, View, HighLog, HighOp, HighCommit, Epoch)
             || Replica <- Config, is_pid(Replica)],
            lager:info("~p view change successful", [self()]),
            {noreply, State?S{status = normal, op = HighOp,
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
               config = Config, mod_state = ModState,
               mod = Mod, next_state = NextState} = State) ->
    %% replace log with Log, set op_num to highest op in the log, set
    %% view_num to View, if there are uncommitted operations in the
    %% log, send prepare_ok messages to the primary for them, commit
    %% any known commited and update commit number.
    Primary = find_primary(Config),
    Commits = sync_log(LocalCommit, LocalLog, Op, Log),
    {NextState1, ModState1} = do_upcalls(Mod, NextState, ModState, LocalLog,
                                         Commits),
    [prepare_ok(Primary, View, PrepOp, Epoch)
     || PrepOp <- lists:seq(Commit + 1, Op)],
    {noreply, State?S{op = Op, view = View, status = normal,
                      mod_state = ModState1, next_state = NextState1}};
handle_cast(#msg{payload=out_of_date, view = View, epoch = Epoch},
            ?S{config = Config} = State) ->
    %% moderately sure that epoch handling here is wrong.  how to test?
    lager:info("~p out_of_date", [self()]),
    Nonce = crypto:rand_bytes(16),
    [recovery(Replica, Nonce, View, Epoch)
     || Replica <- Config, is_pid(Replica), Replica /= self()],
    {noreply, State?S{view = View, epoch = Epoch,
                      status = recovering, nonce = Nonce}};
handle_cast(#msg{payload=#recovery{nonce = Nonce}, sender = Sender},
            ?S{primary = false, view = View, epoch = Epoch,
               config = Config} = State) ->
    %% if status is normal, send recovery_response
    lager:info("~p recovery, non-primary", [self()]),
    Config1 = unfail_replica(Sender, Config),
    recovery_response(Sender, View, Nonce, undefined,
                      undefined, undefined, Epoch),
    {noreply, State?S{config = Config1}};
handle_cast(#msg{payload=#recovery{nonce = Nonce}, sender = Sender},
            ?S{primary = true, view = View, log = Log0,
               op = Op, commit = Commit, epoch = Epoch,
               config = Config} = State) ->
    %% if primary, include additional information
    lager:info("~p recovery, primary", [self()]),
    Config1 = unfail_replica(Sender, Config),
    Log = ship_log(Log0),
    recovery_response(Sender, View, Nonce, Log, Op, Commit, Epoch),
    {noreply, State?S{config = Config1}};
handle_cast(#msg{payload=#recovery_response{nonce = Nonce} = Msg,
                 view = View, sender = Sender, epoch = Epoch},
            ?S{log = LocalLog, commit = LocalCommit, mod = Mod,
               next_state = NextState, mod_state = ModState,
               pending_replies = Pending} = State) ->
    lager:info("~p recovery_response ~p", [self(), Sender]),
    %% have we got f + 1 incuding the primary response? if no wait
    F = f(State?S.config),
    OKs0 = scan_pend(recovery_response, Nonce, View, Pending),
    lager:info("~p recovery_response: ~p, ~p N ~p F ~p",
               [self(), Msg, length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | Pending],
    {Primary, HasPrimary} = has_primary(OKs),
    case length(OKs) of
        N when N >= F + 1 andalso HasPrimary ->
            #recovery_response{log = Log, op = Op, commit = Commit} = Primary,
            %% if yes, use primary values, set to normal, restart participation
            _ = trunc_uncommitted(LocalLog, LocalCommit),
            Commits = sync_log(LocalCommit, LocalLog, Op, Log),
            {NextState1, ModState1} = do_upcalls(Mod, NextState, ModState, LocalLog,
                                                 Commits),
            %% if we're the recovering primary of this quorum, we need
            %% to initiate another view change
            View1 =
                case State?S.primary of
                    true ->
                        [start_view_change(R, View + 1, Epoch)
                         || R <- State?S.config],
                        View + 1;
                    _ -> View
                end,
            lager:info("~p recover successful", [self()]),
            {noreply, State?S{status = normal, next_state = NextState1,
                              mod_state = ModState1, op = Op,
                              commit = Commit, view = View1}};
        _ ->
            {noreply, State?S{pending_replies = [Msg|Pending]}}
    end;
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
    lager:warning("~p unexpected cast ~p", [self(), _Msg]),
    {noreply, State}.

handle_info(timeout, ?S{primary = true, view = View, epoch = Epoch,
                        commit = Commit, config = Config} = State) ->
    %% send commit message to all replicas
    [commit(R, Commit, View, Epoch)
     || R <- Config, is_pid(R), R /= self()],
    {noreply, State};
handle_info(Msg, ?S{primary = false, view = View, epoch = Epoch} = State)
  when Msg =:= timeout -> % need to monitor primary and catch monitor
                                                % failures here
    %% initiate the view change by incrementing the view number and
    %% sending a start_view change message to all reachable replicas.
    lager:info("~p detected primary timeout, starting view change",
               [self()]),
    [_|Config] = State?S.config,
    [start_view_change(R, View + 1, Epoch)
     || R <- Config],
    {noreply, State?S{view = View + 1}};
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

get_client(Pid, Table) ->
    case ets:lookup(Table, Pid) of
        [Client] ->
            Client;
        _ ->
            #client{address = Pid}
    end.

recent(Client, Request, Table) ->
    case ets:lookup(Table, Client) of
        [Record] ->
            in_requests(Request, Record#client.requests);
        _ ->
            false
    end.

in_requests(Request, Requests) ->
    Scan = [R || R <- maps:to_list(Requests),
                 R#request.req_num == Request],
    case Scan of
        [Record] ->
            #request{op = Op,
                     reply = Reply} = Record,
            case Op of
                #operation{committed = true} ->
                    {true, Reply};
                _ ->
                    false
            end;
        [] ->
            false
    end.

get_request(#client{requests = Requests} = _C, Op, _View) ->
    lager:info("~p get_request c ~p o ~p v ~p",
               [self(), _C, Op, _View]),
    Req = [R || {_, #request{op = O} = R}
                    <- maps:to_list(Requests),
                O == Op],
    case Req of
        [#request{} = RR] ->
            RR;
        _E ->
            error({from, _E})
    end.

add_request(#client{requests = Requests} = Client,
            #request{req_num = RequestNum} = Request, Table) ->
    ets:insert(Table,
               Client#client{requests = maps:put(RequestNum,
                                                 Request,
                                                 Requests)}).

add_reply(Client, RequestNum, Reply, Table) ->
    [CR] = ets:lookup(Table, Client),
    #client{requests = Requests} = CR,
    Request = maps:get(RequestNum, Requests),
    Requests1 = maps:put(RequestNum,
                         Request#request{reply = Reply},
                         Requests),
    ets:insert(Table, CR#client{requests = Requests1}).

add_log_entry(Entry, Log) ->
    lager:info("adding log entry ~p", [Entry]),
    ets:insert(Log, Entry).

get_log_entry(Op, Log) ->
    lager:debug("~p looking up log entry ~p", [self(), Op]),
    case ets:lookup(Log, Op) of
        [OpRecord] ->
            OpRecord;
        _Huh -> error({noes, _Huh})
    end.

maybe_catchup(Commit, ?S{commit = LocalCommit} = State)
  when LocalCommit >= Commit ->
    State;
maybe_catchup(Commit, ?S{mod = Mod, next_state = NextState,
                         mod_state = ModState, log = Log,
                         commit = LocalCommit} = State) ->
    {NextState1, ModState1} = do_upcalls(Mod, NextState, ModState, Log,
                                         LocalCommit + 1, Commit),
    State?S{next_state = NextState1, mod_state = ModState1,
            commit = Commit}.

do_upcalls(Mod, NextState, ModState, Log, Start, Stop) ->
    do_upcalls(Mod, NextState, ModState, Log, lists:seq(Start, Stop)).

do_upcalls(Mod, NextState, ModState, Log, Commits) ->
        lists:foldl(fun(Op, {NState, MState}) ->
                            Entry = get_log_entry(Op, Log),
                            Cmd = Entry#operation.command,
                            case Mod:NState(Cmd, MState) of
                                {reply, _, NState1, MState1} ->
                                    update_log_commit_state(Op, Log),
                                    {NState1, MState1};
                                {next_state, NState1, MState1} ->
                                    update_log_commit_state(Op, Log),
                                    {NState1, MState1}
                            end
                    end,
                    {NextState, ModState},
                    Commits).

f(Config) ->
    L = length(Config),
    (L-1) div 2.

%% manually unroll this because the compiler is not smart
scan_pend(prepare_ok, Op, View, Pending) ->
    lager:debug("scan ~p ~p ~p", [Op, View, Pending]),
    [R || #prepare_ok{view = V, op = O} = R <- Pending,
          O == Op andalso V == View];
scan_pend(do_view_change, _Op, View, Pending) ->
    lager:info("scan ~p ~p", [View, Pending]),
    [R || #do_view_change{view = V} = R <- Pending,
          V == View];
scan_pend(recovery_response, Nonce, _, Pending) ->
    lager:info("scan ~p ~p", [Nonce, Pending]),
    [R || #recovery_response{nonce = N} = R <- Pending,
          N =:= Nonce];
scan_pend(_Msg, _Op, _View, _Pending) ->
    error(unimplemented).

clean_pend(prepare_ok, Op, View, Pending) ->
    [R || {#prepare_ok{view = V, op = O} = R, _} <- Pending,
          O == Op, V == View];
clean_pend(_Msg, _Op, _View, _Pending) ->
    error(unimplemented).

add_snapshot(Snap, Op, Log) ->
    %% this is the worst
    ets:insert(Log, {Op, snapshot, Snap}).

%% get_snapshot(Log) ->
%%     [{Op, _, Snap}] = ets:lookup(Log, snapshot),
%%     {Op, Snap}.

update_log_commit_state(Op, Log) ->
    [Operation] = ets:lookup(Log, Op),
    lager:debug("setting ~p ~p to committed", [Op, Operation]),
    ets:insert(Log, Operation#operation{committed = true}),
    %% lager:debug("wtf ~p", [ets:foldr(fun(X, Acc) -> [X|Acc] end, [], Log)]),
    Interval = vrrm:config(snapshot_op_interval),
    maybe_compact(Op, Interval, Log).

ship_log(Log) ->
    ets:foldl(fun(X, Acc) -> [X|Acc] end, [], Log).

trunc_uncommitted(Log, Commit) ->
    case ets:lookup(Log, Commit) of
        [_] ->
            ets:delete(Log, Commit + 1);
        [] ->
            ok
    end.

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
             [#operation{} = OldOp]->
                 NewOp = lists:keyfind(Op, 2, NewLog),
                 %% assert that commands are the same for safety
                 NewCmd = NewOp#operation.command,
                 OldCmd = OldOp#operation.command,
                 lager:info("~p n ~p o ~p", [Op, NewCmd, OldCmd]),
                 NewCmd = OldCmd,
                 Op
         end
         || Op <- lists:seq(OldCommit + 1, NewOpNum)],
    lists:flatten(Ops0).

maybe_compact(Op, Interval, _Log) when Op rem Interval /= 0 ->
    ok;
maybe_compact(_Op, _, _Log) ->
    %% make current app state the snapshot
    %% remove all log entries before Op
    ok.

get_highest_op(Msgs) ->
    lager:info("get highest ~p", [Msgs]),
    get_highest_op(Msgs, 0, undefined).

get_highest_op([], _HighOp, HighMsg) ->
    HighMsg;
get_highest_op([H|T], HighOp, HighMsg) ->
    case H#do_view_change.op > HighOp of
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

primary_timeout() ->
    vrrm:config(idle_commit_interval).

replica_timeout() ->
    jitter(20, vrrm:config(primary_failure_interval)).

jitter(Pct, Num) ->
    M = 100 + (random:uniform(Pct * 2) - Pct),
    (M * Num) div 100.
