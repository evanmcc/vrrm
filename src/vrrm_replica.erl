-module(vrrm_replica).

-behaviour(gen_statem).

%% API
-export([
         start_link/2, start_link/4
        ]).

%% gen_statem callbacks
-export([
         init/1,
         callback_mode/0,
         handle_event/4,
         terminate/2,
         code_change/3
        ]).

%%% messages
-export([
         %% async
         prepare/9,
         prepare_ok/4,
         commit/5,

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
         get_status/1,
         reconfigure/3,
         get_mod_state/1,
         state_transfer/1,
         am_primary/1
         %% get_op/2, get_op/3,
         %% swap_node/2
        ]).

%% should rewrite this to use the statem
-type nodename() :: pid() | atom().
-export_type([nodename/0]).
-callback init(Args::[term()]) ->
    {ok, StateName::atom(), ModState::term()} |
    {error, Reason::term()}.

%% any number of states can defined by the behavior, which must
%% express them:
%% Mod:StateName(Event::term(), Role::atom(), ModState::term()) ->
%%     {next_state, NextStateName, UpdModState} |
%%     {reply, Reply, NextStateName, UpdModState} |
%%     {stop, UpdModState}.

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
          epoch :: non_neg_integer(),
          view :: non_neg_integer() | undefined,
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
          client :: pid()
        }).
-type operation() :: #operation{}.

-record(request,
        {
          req_num :: non_neg_integer(),
          op :: operation(),
          view :: non_neg_integer(),
          reply :: term() | undefined,
          from :: tuple() | undefined
        }).
-type request() :: #request{}.

-record(replica_data,
        {
          %% replication state
          primary = false :: boolean(),
          config = [] :: [nodename()],

          timeout = make_ref() :: reference(),

          view = 0 :: non_neg_integer(),
          last_normal = 0 :: non_neg_integer(),
          commits = [] :: [non_neg_integer()],
          min_commit = 0 :: non_neg_integer(),
          log = ets:new(log,
                        [set,
                         {keypos, 2},
                         private]) :: ets:tid(),
          op =  0 :: non_neg_integer(),
          commit = 0 :: non_neg_integer(),
          client_table = #{} :: #{pid() => client()},
          epoch = 0 :: non_neg_integer(),
          old_config = [] :: [nodename()],

          pending_replies = [] :: [any()],
          nonce :: binary() | undefined,

          %% callback module state
          mod :: atom(),
          next_state :: atom(),
          mod_state :: term()
        }).

-define(D, #replica_data).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Mod, ModArgs) ->
    start_link(Mod, ModArgs, true, #{}).

-spec start_link(atom(), [term()], boolean(), #{}) ->
                        {ok, pid()} |
                        {error, Reason::term()}.
start_link(Mod, ModArgs, New, Opts) ->
    gen_statem:start_link(?MODULE, [Mod, ModArgs, New, Opts], []).

%% request is should be abstracted to the vrrm_client module, mostly.
-spec request(pid(), term(), vrrm_client:id(), non_neg_integer()) ->
                     {ok, Reply::term(), Epoch::non_neg_integer()} |
                     {error, not_primary, Primary::nodename()} |
                     {error, Reason::term()}.
request(Primary, Command, Request) ->
    request(Primary, Command, Request, 500).

request(Primary, Command, Request, Timeout) ->
    lager:info("request ~p", [Request]),
    try
        gen_statem:call(Primary, {request, Command, self(), Request}, Timeout)
    catch C:E ->
            lager:warning("request failed with: ~p:~p", [C,E]),
            {error, timeout}
    end.

initial_config(Replica, Config1) ->
    Config = stable_sort(Config1),
    %Config = list_to_tuple(Config0),
    gen_statem:call(Replica, {initial_config, Config}, infinity).

get_config(Replica) ->
    gen_statem:call(Replica, get_config).

get_status(Replica) ->
    gen_statem:call(Replica, get_status).

reconfigure(Primary, NewConfig1, Request) ->
    NewConfig = stable_sort(NewConfig1),
    %%NewConfig = list_to_tuple(NewConfig0),
    gen_statem:call(Primary, {reconfigure, NewConfig, self(), Request},
                    infinity).

get_mod_state(Replica) ->
    gen_statem:call(Replica, get_mod_state).

am_primary(Replica) ->
    gen_statem:call(Replica, am_primary).

-record(st, {view :: non_neg_integer(),
             commit :: non_neg_integer(),
             log :: [term()]}).
state_transfer(Replica) ->
    gen_statem:call(Replica, state_transfer, infinity).

%% internal messages
-record(prepare, {primary :: nodename(), command :: term(),
                  client :: pid(), request :: non_neg_integer(),
                  op :: non_neg_integer(), commit :: non_neg_integer(),
                  min_commit :: non_neg_integer()}).
prepare(Replica, View, Command, Client, Request, Op, Commit, MinCommit, Epoch) ->
    gen_statem:cast(Replica,
                    #msg{view = View, epoch = Epoch, sender = self(),
                         payload =
                             #prepare{primary = self(), op = Op,
                                      client = Client, request = Request,
                                      command = Command, commit = Commit,
                                      min_commit = MinCommit}}).

-record(prepare_ok, {op :: non_neg_integer(), sender :: pid(),
                     view :: non_neg_integer()}).
prepare_ok(Replica, View, Op, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #prepare_ok{op = Op,
                                                  view = View, % required for filtering
                                                  sender = self()}}).

-record(commit, {commit :: non_neg_integer(), min_commit :: non_neg_integer()}).
commit(Replica, Commit, MinCommit, View, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #commit{commit = Commit,
                                              min_commit = MinCommit}}).

-record(start_view_change, {view :: non_neg_integer(), sender :: pid(),
                            epoch :: non_neg_integer()}).
start_view_change(Replica, View, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #start_view_change{view = View,
                                                         epoch = Epoch,
                                                         sender = self()}}).

-record(do_view_change, {view :: non_neg_integer(), log :: term(),
                         old_view :: non_neg_integer(), op :: non_neg_integer(),
                         commit :: non_neg_integer(), sender :: pid()}).
do_view_change(NewPrimary, View, Log, OldView, Op, Commit, Epoch) ->
    gen_statem:cast(NewPrimary, #msg{view = View, epoch = Epoch, sender = self(),
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
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #start_view{view = View, log = Log,
                                                  op = Op, commit = Commit}}).

out_of_date(Replica, View, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload = out_of_date}).

-record(recovery, {nonce :: binary()}).
recovery(Replica, Nonce, View, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #recovery{nonce = Nonce}}).

-record(recovery_response, {nonce :: binary(), log :: [term()] | undefined,
                            op :: non_neg_integer() | undefined,
                            commit :: non_neg_integer() | undefined}).
recovery_response(Replica, View, Nonce, Log, Op, Commit, Epoch) ->
    gen_statem:cast(Replica, #msg{view = View, epoch = Epoch, sender = self(),
                                  payload =
                                      #recovery_response{nonce = Nonce, op = Op,
                                                         log = Log,
                                                         commit = Commit}}).

-record(start_epoch, {op :: non_neg_integer(), old_config :: [pid()],
                      new_config :: [pid()]}).
start_epoch(Replica, Epoch, Op, OldConfig, NewConfig) ->
    gen_statem:cast(Replica, #msg{epoch = Epoch, sender = self(),
                                  payload =
                                      #start_epoch{op = Op,
                                                   old_config = OldConfig,
                                                   new_config = NewConfig}}).

-record(epoch_started, {epoch :: non_neg_integer()}).
epoch_started(Replica, Epoch) ->
    gen_statem:cast(Replica, #msg{epoch = Epoch, sender = self(),
                                  payload =
                                      #epoch_started{epoch = Epoch}}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() ->
    handle_event_function.

init([Mod, ModArgs, New, _Opts]) ->
    rand:seed(exs64),
    case New of
        true ->
            %% some ugly duplication here; is there a better way to do
            %% this?
            case Mod:init(ModArgs) of
                {ok, NextState, ModState} ->
                    State = ?D{mod = Mod,
                               next_state = NextState,
                               mod_state = ModState},
                    add_snapshot({NextState, Mod:serialize(ModState)},
                                 0, State?D.log),
                    {ok, unconfigured, State};
                {ok, NextState, ModState, Timeout} ->
                    State = ?D{mod = Mod,
                               next_state = NextState,
                               mod_state = ModState},
                    add_snapshot({NextState, Mod:serialize(ModState)},
                                 0, State?D.log),
                    {ok, unconfigured, State, Timeout}
            end;
        false ->
            %% here we need to create a node without any
            %% configuration or state (since it'll just get thrown
            %% away), then I guess do a reconfiguration
            throw(oh_god)
    end.

%% need to figure out if we want state_enter calls

%% request
handle_event({call, _From},
             {request, _Command, _Client, _Request},
             unconfigured, Data) ->
    lager:info("recieved a request while unconfigured"),
    {next_state, unconfigured, Data, [postpone]};
handle_event({call, _From},
             _, %{request, _Command, _Client, _Request},
             recovering, Data) ->
    lager:info("postponing request to recovering node"),
    {next_state, recovering, Data};
handle_event({call, From}, {initial_config, Config}, unconfigured, Data) ->
    %% should check that we're in unconfigured+config=undefined
    Primary = find_primary(Config, 0),
    AmPrimary = self() =:= Primary,
    lager:info("~p inserting initial configuration, primary: ~p, ~p",
                [self(), Primary, AmPrimary]),
    Timeout =
        case AmPrimary of
            true ->
                primary_timeout(make_ref());
            false ->
                %% haxx
                replica_timeout(make_ref())
        end,
    {next_state, normal,
     Data?D{config = Config,
            commits = lists:duplicate(length(Config), 0),
            primary = AmPrimary,
            timeout = Timeout},
     [{reply, From, ok}]};
%% events in normal
handle_event({call, From},
             {request, _Command, _Client, _Request},
             State,
             ?D{primary = false, config = Config, view = View} = Data)
  when State =:= normal;
       State =:= transitioning ->
    lager:info("recieved a request when not primary"),
    Primary = find_primary(Config, View),
    {next_state, normal, Data,
     [{reply, From, {error, not_primary, Primary}}]};
handle_event({call, From},
             {request, _Command, _Client, _Request},
             transitioning,
             ?D{primary = true} = Data) ->
    lager:info("recieved a request while reconfiguring"),
    {next_state, transitioning, Data,
     [{reply, {error, From, reconfiguring}}]};
handle_event({call, From},
             {request, Command, Client, Request},
             normal,
             ?D{log = Log, commit = Commit, timeout = Timeout,
                view = View, op = Op, epoch = Epoch,
                client_table = Table, config = Config,
                commits = Commits} = Data) ->
    lager:info("~p request: command ~p client ~p request ~p",
                [self(), Command, Client, Request]),
    %% compare with recent requests table, resend reply if
    %% already completed
    case recent(Client, Request, Table) of
        {true, Reply} ->
            {next_state, normal, Data, [{reply, From, Reply}]};
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
            RequestRecord = #request{op = Entry,
                                     view = View,
                                     req_num = Request,
                                     from = From},
            MinCommit = find_min_commit(Commits),
            Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
            [prepare(Replica, View, Command, Client, Request,
                     Op1, Commit, MinCommit, Epoch)
             || Replica <- Config, Replica /= self(), is_pid(Replica)],
            {next_state, normal,
             Data?D{op = Op1,
                    client_table = Table1,
                    timeout = primary_timeout(Timeout)}}
    end;
handle_event({call, From},
             get_config,
             normal,
             ?D{primary = false, view = View, config = Config} = Data) ->
    Primary = find_primary(Config, View),
    {next_state, normal, Data, [{reply, From, {error, not_primary, Primary}}]};
handle_event({call, From}, get_config, normal, ?D{config = Config} = Data) ->
    {next_state, normal, Data, [{reply, From, {ok, Config}}]};
%% is this safe on non-primary nodes?
handle_event({call, From}, get_status, State, Data) ->
    {next_state, normal, Data, [{reply, From, {ok, State}}]};
handle_event({call, From}, {reconfigure, _NewConfig, _Client, _Request}, normal,
            ?D{primary = false, config = Config, view = View} = Data) ->
    Primary = find_primary(Config, View),
    {next_state, normal, Data, [{reply, From, {error, not_primary, Primary}}]};
%% there is duplicate code here, do we want to merge it with normal
%% request handling as a special case?
handle_event({call, From},
             {reconfigure, NewConfig, Client, Request},
             normal,
            ?D{config = Config, primary = true,
               log = Log, client_table = Table, view = View,
               timeout = Timeout, commits = Commits,
               commit = Commit, op = Op, epoch = Epoch} = Data) ->
    %% validate new config, Epoch == epoch, and Request is not already
    %% processed for this Client (only valid on primary).
    %% if that's all OK: insert a new, special internal operation into
    %% the consensus log & do the normal stuff, then stop accepting
    %% client requests (queue, then deal with them after
    %% start_epoch?). when that completes, the prepare_ok step will
    %% send start_epoch to new nodes

    case recent(Client, Request, Table) of
        {true, Reply} ->
            {next_state, normal, Data, [{reply, From, Reply}]};
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
            RequestRecord = #request{op = Entry,
                                     view = View,
                                     req_num = Request,
                                     from = From},
            Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
            MinCommit = find_min_commit(Commits),
            [prepare(Replica, View, {'$reconfigure', NewConfig}, Client,
                     Request, Op1, Commit, MinCommit, Epoch)
             || Replica <- Config, Replica /= self(), is_pid(Replica)],
            {next_state, transitioning,
             Data?D{op = Op1, client_table = Table1,
                    timeout = primary_timeout(Timeout)}}
    end;
handle_event({call, From}, get_mod_state, normal,
            ?D{next_state = Next, mod_state = Mod} = Data) ->
    %% TODO: we need to move to serialize here!
    {next_state, normal, Data, [{reply, From, {Next, Mod}}]};
handle_event({call, From},
             state_transfer,
             State,
            ?D{log = Log, commit = Commit, view = View} = Data)
  when State =:= transitioning orelse State =:= normal ->
    ST = #st{log = ship_log(Log, 0),
             commit = Commit,
             view = View},
    {next_state, normal, Data, [{reply, From, {ok, ST}}]};
handle_event({call, From}, am_primary, normal, ?D{primary = Primary} = Data) ->
    {next_state, normal, Data, [{reply, From, {ok, Primary}}]};
%% cast responses
handle_event(cast, #msg{epoch = Epoch, sender = Sender,
                        payload = _Payload} = _Msg,
             normal,
             ?D{view = LocalView, epoch = LocalEpoch} = Data)
  when LocalEpoch > Epoch ->
    lager:info("e sending ood message, got local e ~p v ~p msg ~p",
               [LocalEpoch, LocalView, _Msg]),
    out_of_date(Sender, LocalView, LocalEpoch),
    {next_state, normal, Data};
handle_event(cast, #msg{view = View, epoch = Epoch, sender = Sender,
                        payload = _Payload} = _Msg,
             normal,
             ?D{view = LocalView, epoch = LocalEpoch} = Data)
  when LocalEpoch =:= Epoch andalso LocalView > View ->
    lager:info("sending ood message, got local e ~p v ~p msg ~p",
               [LocalEpoch, LocalView, _Msg]),
    out_of_date(Sender, LocalView, LocalEpoch),
    {next_state, normal, Data};
handle_event(cast, #msg{payload = Payload} = _Msg,
             recovering, Data)
  when not is_record(Payload, recovery) andalso
       not is_record(Payload, recovery_response) ->
    %% ignore other protocols when we're in recovery mode.
    lager:debug("~p discarding non-recovery message: ~p", [self(), _Msg]),
    {next_state, recovering, Data, [postpone]};
handle_event(cast,
             #msg{payload = #prepare{primary = Primary, command = Command,
                                     client = Client, request = Request,
                                     min_commit = MinCommit,
                                     op = Op, commit = Commit},
                  view = View} = Msg,
             normal,
             ?D{timeout = Timeout} = Data0) ->
    lager:debug("~p prepare: command ~p", [self(), Msg]),
    %% is log complete? consider state transfer if not
    %% if CommitNum is higher than commit_num, do some upcalls
    Data = maybe_catchup(Commit, Data0),
    Table = Data?D.client_table,
    ClientRecord = get_client(Client, Table),
    Entry = #operation{num = Op,
                       client = Client,
                       view = View,
                       command = Command,
                       req_num = Request},
    RequestRecord = #request{op = Entry,
                             view = View,
                             req_num = Request},
    Table1 = add_request(Client, ClientRecord, RequestRecord, Table),
    %% increment op number, append operation to log, update client table(?)
    _ = add_log_entry(Entry, Data?D.log),
    %% reply with prepare_ok
    prepare_ok(Primary, View, Op, Msg#msg.epoch),
    {next_state, normal,
     Data?D{op = Op,
            client_table = Table1,
            min_commit = MinCommit,
            timeout = replica_timeout(Timeout)}};
handle_event(cast,
             #msg{payload = #prepare_ok{op = Op}, sender = Sender},
             State,
             ?D{commit = Commit, config = Config,
                commits = Commits} = Data)
  when Commit >= Op andalso
       (State =:= normal orelse State =:= transitioning) ->
    %% even if this is old and we've already committed the operation
    %% locally, we need to keep track of the commit for view change
    %% optimization.
    RemoteCommit = Op - 1,
    Commits1 = update_commits(Sender, Config, RemoteCommit, Commits),
    {next_state, normal, Data?D{commits = Commits1}};
handle_event(cast,
             #msg{payload = #prepare_ok{op = Op},
                  sender = Sender} = Msg,
             State,
             ?D{mod = Mod, next_state = NextState, mod_state = ModState,
                client_table = Table, log = Log, view = View,
                pending_replies = Pending, config = Config,
                epoch = Epoch, commits = Commits} = Data)
  when State =:= normal orelse State =:= transitioning ->
    RemoteCommit = Op - 1,
    Commits1 = update_commits(Sender, Config, RemoteCommit, Commits),
    %% do we have f replies? if no, wait for more (or timeout)
    F = f(Data?D.config),
    OKs = scan_pend(prepare_ok, {Op, View}, Pending),
    lager:info("~p prepare_ok: ~p, ~p N ~p F ~p ~p",
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
                    lager:info("~p reconfigure succeded", [self()]),
                    NewNodes = NewConfig -- Config,
                    Epoch1 = Epoch + 1,
                    [start_epoch(Replica, Epoch1, Op, Config, NewConfig)
                     || Replica <- NewNodes,
                        is_pid(Replica),
                        Replica /= self()],
                    %% send a commit to the remaining nodes so we
                    %% don't have to wait for one
                    MinCommit = find_min_commit(Commits1),
                    [commit(Replica, Op, MinCommit, 0, Epoch1)
                     || Replica <- Config -- NewNodes,
                        Replica /= self(),
                        is_pid(Replica)],
                    Reply = ok,
                    View1 = 0,
                    NextState1 = NextState,
                    ModState1 = ModState,
                    %% even it we're not the new primary, we can leave
                    %% our commits list as-is.
                    AmPrimary = self() =:= find_primary(NewConfig, 0);
                _ ->
                    %% this needs to grow proper handling at some point
                    {reply, Reply, NextState1, ModState1} =
                        Mod:NextState(Command, ModState),
                    View1 = View,
                    Epoch1 = Epoch,
                    AmPrimary = true
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
                    gen_statem:reply(From, {ok, Reply, Epoch})
            end,
            _ = add_reply(Client, ReqRec#request.req_num, Reply, Table),
            Pending1 = clean_pend(prepare_ok, {Op, View}, Pending),
            %% do we need to check if commit == Op - 1?
            {next_state, normal,
             Data?D{commit = Op, epoch = Epoch1, view = View1,
                    mod_state = ModState1, next_state = NextState1,
                    pending_replies = Pending1, primary = AmPrimary,
                    commits = Commits1}};
        _ ->
            %% wait for more
            %% addendum: this is bad FIXME, need to dedupe!
            Pending1 = [Msg|Pending],
            {next_state, normal, Data?D{pending_replies = Pending1,
                                        commits = Commits1}}
    end;
handle_event(cast,
             #msg{payload=#commit{commit = Commit, min_commit = MinCommit},
                  view = View, epoch = Epoch},
             State,
             ?D{timeout = Timeout, commits = Commits} = Data1)
  when State =:= transitioning orelse State =:= normal ->
    %% if Commit > commit_num, do upcalls, doing state transfer if
    %% needed.
    Data0 = Data1?D{min_commit = MinCommit},
    CurrentEpoch = Data0?D.epoch,
    case Epoch of
        CurrentEpoch ->
            Data = maybe_catchup(Commit, Data0),
            {next_state, normal,
             Data?D{timeout = replica_timeout(Timeout)}};
        _ ->
            Data = maybe_transition(Commit, Epoch, Data0),
            Commits1 =
                case self() =:= find_primary(Data?D.config, View) of
                    false -> Commits;
                    _ -> lists:duplicate(length(Data?D.config), Commit)
                end,
            {next_state, transitioning, Data?D{commits = Commits1}}
    end;
handle_event(cast,
             #msg{payload=#start_view_change{}, view=View, epoch=Epoch},
             normal,
             ?D{op = Op, view = OldView,
                config = Config, log = Log0,
                min_commit = MinCommit,
                commit = Commit, timeout = Timeout} = Data) ->
    lager:info("~p start_view_change: ~p", [self(), View]),
    Log = ship_log(Log0, MinCommit),
    Primary = find_primary(Config, View), % use the new view
    lager:info("~p sending do view change ~p -> ~p to ~p",
               [self(), OldView, View, Primary]),
    do_view_change(Primary, View, Log, OldView, Op, Commit, Epoch),
    %% change status to view_change if not already there, and note last_normal
    %% reset interval change to avoid storm?
    {next_state, view_change,
     Data?D{last_normal = OldView, timeout = replica_timeout(Timeout)}};
handle_event(cast,
             #msg{payload=#start_view_change{}, view=View, epoch=Epoch},
             view_change,
             ?D{op = Op, view = OldView,
                config = Config, log = Log0,
                min_commit = MinCommit,
                commit = Commit, timeout = Timeout} = Data) ->
    lager:info("~p start_view_change: ~p", [self(), View]),
    Log = ship_log(Log0, MinCommit),
    Primary = find_primary(Config, View), % use the new view
    lager:info("~p sending do view change ~p -> ~p to ~p",
               [self(), OldView, View, Primary]),
    do_view_change(Primary, View, Log, OldView, Op, Commit, Epoch),
    %% reset interval change to avoid storm?
    {next_state, view_change, Data?D{timeout = replica_timeout(Timeout)}};
handle_event(cast,
             #msg{payload = #do_view_change{view = View}},
             normal,
             ?D{view = OurView} = Data) when View =< OurView->
    %% these are late, drop them.
    {next_state, normal, Data};
handle_event(cast,
             #msg{payload = #do_view_change{view = View} = Msg},
             view_change,
             ?D{pending_replies = Pending, epoch = Epoch,
                log = Log, config = Config, commit = Commit,
                timeout = Timeout} = Data) ->
    %% have we gotten f + 1 (inclusive of self)? if no, wait
    F = f(Data?D.config),
    OKs0 = scan_pend(do_view_change, View, Pending),
    lager:info("~p do_view_change: ~p N ~p F ~p",
                [self(), length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    HasSelf = has_self(OKs, self()),
    %% assertion
    true = self() == find_primary(Config, View),
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
             || Replica <- Config, is_pid(Replica) /= self()],
            lager:info("view change successful: ~p", [View]),
            Pending1 = clean_pend(do_view_change, View, Pending),
            {next_state, normal,
             Data?D{op = HighOp, view = View,
                    pending_replies = Pending1,
                    primary = true, commit = HighCommit,
                    timeout = primary_timeout(Timeout)}};
        _ ->
            Pending1 = [Msg | Pending],
            {next_state, view_change, Data?D{pending_replies = Pending1}}
    end;
handle_event(cast,
             #msg{payload=#start_view{view = View, log = Log, op = Op,
                                      commit = Commit},
                  epoch = Epoch},
             view_change,
             ?D{log = LocalLog, commit = LocalCommit,
                config = Config, mod_state = ModState,
                old_config = OldConfig, timeout = Timeout,
                min_commit = MinCommit,
                mod = Mod, next_state = NextState} = Data) ->
    %% replace log with Log, set op_num to highest op in the log, set
    %% view_num to View, if there are uncommitted operations in the
    %% log, send prepare_ok messages to the primary for them, commit
    %% any known commited and update commit number.
    lager:info("start view ~p", [View]),
    Primary = find_primary(Config, View),
    AmPrimary = Primary =:= self(),
    Commits = sync_log(LocalCommit, LocalLog, Op, Log),
    {NextState1, ModState1, {OldConfig1, Config1}}
        = do_upcalls(Mod, NextState, ModState, LocalLog,
                     {OldConfig, Config}, Commits),
    [prepare_ok(Primary, View, PrepOp, Epoch)
     || PrepOp <- lists:seq(Commit + 1, Op)],
    MinCommits =
        case AmPrimary of
            false ->
                [];
            true ->
                lists:duplicate(length(Config), MinCommit)
        end,
    {next_state, normal,
     Data?D{op = Op, view = View,
            config = Config1, old_config = OldConfig1,
            mod_state = ModState1, next_state = NextState1,
            primary = AmPrimary, commits = MinCommits,
            timeout = select_timeout(AmPrimary, Timeout)}};
handle_event(cast,
             #msg{payload=out_of_date, view = View, epoch = Epoch},
             normal,
             ?D{view = View, epoch = Epoch} = Data) ->
    {next_state, normal, Data};
handle_event(cast,
             #msg{payload=out_of_date, view = View, epoch = Epoch},
             normal,
             ?D{config = Config, view = _OurView, epoch = _OurEpoch} = Data) ->
    %% moderately sure that epoch handling here is wrong.  how to test?
    lager:info("out_of_date"),
    Nonce = crypto:strong_rand_bytes(16),
    [recovery(Replica, Nonce, View, Epoch)
     || Replica <- Config, is_pid(Replica), Replica /= self()],
    {next_state, recovering,
     Data?D{view = View, epoch = Epoch,
            nonce = Nonce}};
handle_event(cast,
             #msg{payload=#recovery{nonce = Nonce}, sender = Sender},
             normal,
             ?D{primary = false, view = View, epoch = Epoch} = Data) ->
    %% if status is normal, send recovery_response
    lager:info("recovery, non-primary to ~p", [Sender]),
    recovery_response(Sender, View, Nonce, undefined,
                      undefined, undefined, Epoch),
    {next_state, normal, Data};
handle_event(cast,
             #msg{payload=#recovery{nonce = Nonce}, sender = Sender},
             normal,
             ?D{primary = true, view = View, log = Log0,
                op = Op, commit = Commit, epoch = Epoch} = Data) ->
    %% if primary, include additional information
    lager:info("recovery, primary to ~p", [Sender]),
    %% do we need to ship the entire log here?  We potentially have
    %% this node's last min commit, so we could theoretically use
    %% that.
    Log = ship_log(Log0, 0),
    recovery_response(Sender, View, Nonce, Log, Op, Commit, Epoch),
    {next_state, normal, Data};
handle_event(cast,
             #msg{payload=#recovery_response{nonce = Nonce} = Msg,
                  view = View, sender = _Sender, epoch = Epoch},
             recovering,
             ?D{log = LocalLog, commit = LocalCommit, mod = Mod,
                config = Config, old_config = OldConfig,
                next_state = NextState, mod_state = ModState,
                pending_replies = Pending,
                timeout = Timeout} = Data) ->
    lager:info("recovery_response from ~p", [_Sender]),
    %% have we got f + 1 incuding the primary response? if no wait
    F = f(Data?D.config),
    OKs0 = scan_pend(recovery_response, Nonce, Pending),
    lager:info("recovery_response: ~p, ~p N ~p F ~p",
                [Msg, length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    {Primary, HasPrimary} = has_primary_response(OKs),
    case length(OKs) of
        N when N >= F + 1 andalso HasPrimary ->
            #recovery_response{log = Log, op = Op, commit = Commit} = Primary,
            %% if yes, use primary values, set to normal, restart participation
            _ = trunc_uncommitted(LocalLog, LocalCommit),
            Commits = sync_log(LocalCommit, LocalLog, Op, Log),
            {NextState1, ModState1, {OldConfig1, Config1}} =
                do_upcalls(Mod, NextState, ModState, LocalLog,
                           {OldConfig, Config}, Commits),
            Pending1 = clean_pend(recovery_response, Nonce, Pending),
            case Config1 =/= Config andalso
                not lists:member(self(), Config1) of
                true ->
                    %% the epoch has changed at some point in our log
                    %% replication, we need to check that we still
                    %% exist in the new system.
                    {stop, normal,
                     Data?D{next_state = NextState1,
                             mod_state = ModState1,
                             pending_replies = Pending1}};
                _ ->
                    AmPrimary = self() == find_primary(Config1, View),
                    lager:info("recover successful"),
                    {next_state, normal,
                     Data?D{primary = AmPrimary,
                            next_state = NextState1,
                            mod_state = ModState1, op = Op,
                            config = Config1, old_config = OldConfig1,
                            commit = Commit,
                            view = View, epoch = Epoch,
                            pending_replies = Pending1,
                            timeout = select_timeout(AmPrimary, Timeout)}}
            end;
        _ ->
            {next_state, recovering, Data?D{pending_replies = [Msg|Pending]}}
    end;
handle_event(cast,
             #msg{epoch = Epoch, sender = Sender,
                  payload = #start_epoch{}},
             normal,
             ?D{epoch = Epoch} = Data) ->
    %% if we're normal and in Epoch, reply with epoch_started
    epoch_started(Sender, Epoch),
    {next_state, normal, Data};
handle_event(cast,
             #msg{payload=#start_epoch{op = Op, old_config = OldConfig,
                                       new_config = NewConfig},
                  epoch = Epoch},
             unconfigured,
             ?D{log = Log, mod = Mod} = Data) ->
    %% this state is for non-prewarmed new nodes in a configuration
    %% record Epoch, Op, and configs, set view number = 0, and state
    %% to transitioning^W normal, since we block here forever trying
    %% to catch up..

    %% pick a node at random from the nodes that will be staying
    Source = pick(NewConfig -- (NewConfig -- OldConfig)),
    {ok, ST} = state_transfer(Source),
    lager:info("~p got transfer ~p from ~p", [self(), ST, Source]),
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
    AmPrimary = find_primary(NewConfig, 0),
    {next_state, normal,
     Data?D{epoch = Epoch, primary = AmPrimary,
            old_config = OldConfig, config = NewConfig,
            commit = ST#st.commit, op = Op,
            view = 0, next_state = NextState1,
            mod_state = ModState1}};
handle_event(cast,
             #msg{payload = #epoch_started{} = Msg, epoch = Epoch},
             transitioning,
             ?D{pending_replies = Pending} = Data) ->
    %% if we've gotten f + 1 of these, we die.
    %% TODO: if we don't get enough in time, we re-send start_epoch to
    %% new nodes
    F = f(Data?D.config),
    OKs0 = scan_pend(epoch_started, Epoch, Pending),
    lager:debug("~p epoch_started : ~p, ~p N ~p F ~p",
               [self(), Msg, length(Pending), length(OKs0) + 1, F + 1]),
    OKs = [Msg | OKs0],
    case length(OKs) of
        N when N >= F + 1 ->
            {stop, normal, Data};
        _ ->
            {next_state, transitioning, Data?D{pending_replies = [Msg|Pending]}}
    end;
%% message events

handle_event(info,
             commit_timeout,
             normal,
             ?D{primary = true, view = View, epoch = Epoch,
                commit = Commit, config = Config,
                timeout = Timeout, commits = Commits} = Data) ->
    %% send commit message to all replicas
    MinCommit = find_min_commit(Commits),
    [commit(R, Commit, MinCommit, View, Epoch)
     || R <- Config, is_pid(R), R /= self()],
    {next_state, normal, Data?D{timeout = primary_timeout(Timeout)}};
%% need to monitor primary and catch monitor failures here
handle_event(info,
             primary_timeout,
             normal,
             ?D{primary = false, view = View,
                timeout = Timeout, epoch = Epoch} = Data) ->
    %% initiate the view change by incrementing the view number and
    %% sending a start_view change message to all reachable replicas.
    lager:info("detected primary timeout, starting view change"),
    Config = Data?D.config,
    [start_view_change(R, View + 1, Epoch)
     || R <- Config, is_pid(R)],
    {next_state, view_change,
     Data?D{view = View + 1,
            timeout = replica_timeout(Timeout)}};
%% tunnel user timeouts to the inner fsm
handle_event(info,
             timeout,
             normal,
             ?D{mod = Mod, next_state = NextState,
                mod_state = ModState} = Data) ->
    case Mod:NextState(timeout, ModState) of
        {next_state, NextState1, ModState1} ->
            {next_state, normal,
             Data?D{next_state = NextState1,
                    mod_state = ModState1}};
        {stop, Reason, ModState1} ->
            {stop, Reason, Data?D{mod_state = ModState1}}
    end;
handle_event(info,
             Msg,
             normal,
             ?D{mod = Mod, mod_state = ModState} = Data) ->
    try Mod:handle_info(Msg, ModState) of
        {next_state, NextState1, ModState1} ->
            {next_state, normal,
             Data?D{next_state = NextState1,
                    mod_state = ModState1}};
        {stop, Reason, ModState1} ->
            {stop, Reason, Data?D{mod_state = ModState1}}
    catch error:function_clause ->
            lager:warning("~p unexpected message ~p", [self(), Msg]),
            {next_state, normal, Data}
    end;
handle_event(_Type, _Event, State, Data) ->
    lager:warning("unexpected event ~p of type ~p with state ~p and data ~p",
                  [_Event, _Type, State, Data]),
    {next_state, State, Data}.


terminate(_Reason, _Data) ->
    ok.

code_change(_OldVsn, Data, _Extra) ->
    {ok, Data}.

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
        {ok, Record} ->
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
    %% this is fragile if someone changes this config on a running node
    MaxCache = vrrm:config(per_client_cache_limit, 1000),
    Requests1 = Requests#{RequestNum => Request},
    Requests2 =
        case RequestNum - MaxCache of
            Nothing when Nothing =< 0 ->
               Requests1;
            Take ->
                case maps:take(Take, Requests1) of
                    {_, Req2} -> Req2;
                    _ -> Requests1
                end
        end,
    maps:put(Pid, Client#client{requests = Requests2}, Table).

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
maybe_transition(Commit, Epoch, Data) ->
    maybe_catchup(Commit, Data?D{epoch = Epoch, view = 0}).

maybe_catchup(Commit, ?D{commit = LocalCommit} = Data)
  when LocalCommit >= Commit ->
    Data;
maybe_catchup(Commit, ?D{mod = Mod, next_state = NextState,
                         mod_state = ModState, log = Log,
                         config = Config, old_config = OldConfig,
                         commit = LocalCommit} = Data) ->
    {NextState1, ModState1, {OldConfig1, Config1}}
        = do_upcalls(Mod, NextState, ModState, Log,
                     {OldConfig, Config}, LocalCommit + 1, Commit),
    Data?D{next_state = NextState1, mod_state = ModState1,
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
                 end, Pending).

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

ship_log(Log, MinCommit) ->
    ets:foldl(fun(X = #operation{num = Commit}, Acc) when Commit >= MinCommit ->
                      [X|Acc];
                 (_, Acc) ->
                      Acc
              end, [], Log).

trunc_uncommitted(Log, Commit) ->
    %% this is an operation, rather than the snapshot hack
    ets:select_delete(Log, [{{'_','$1','_','_','_','_','_'},
                             [],[{'>','$1',Commit}]}]).

sync_log(OldCommit, OldLog, NewOpNum, NewLog) ->
    %% get a list of ops
    %% TODO: do we need to take this return and process it before we
    %% catchup at the next prepare/commit?
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

has_primary_response([]) ->
    {meh, false};
has_primary_response([#recovery_response{log=Log}=P|_T]) when Log =/= undefined->
    {P, true};
has_primary_response([_|T]) ->
    has_primary_response(T).

find_primary(Config, View) ->
    lager:info("find ~p ~p", [Config, View]),
    Pos = (View rem length(Config)) + 1,
    lists:nth(Pos, Config).

select_timeout(true, TRef) ->
    lager:info("refreshing prmary timeout"),
    primary_timeout(TRef);
select_timeout(false, TRef) ->
    lager:info("refreshing replica timeout"),
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
    M = 100 + (rand:uniform(Pct * 2) - Pct),
    (M * Num) div 100.

pick(Lst) ->
    lists:nth(rand:uniform(length(Lst)), Lst).

stable_sort(Replicas) ->
    NRs =[{node(Replica), Replica} || Replica <- Replicas],
    lists:sort(NRs),
    [Replica || {_Nodename, Replica} <- NRs].

%% this is a separate function in case we want to later change the
%% implementation of the commit list
find_min_commit(Commits) ->
    lists:min(Commits).

update_commits(Node, Config, Commit, Commits) ->
    Zip = lists:zip(Config, Commits),
    [ case Nd of
          Node ->
              Commit;
          _ ->
              Cmt
     end
     || {Nd, Cmt} <- Zip].
