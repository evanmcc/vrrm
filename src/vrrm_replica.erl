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
         request/3, request/4,
         initial_config/2
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
                        [bag,
                         {keypos, 2},
                         private]) :: ets:tid(),
          snapshot :: term(),
          op =  0 :: non_neg_integer(),
          commit = 0 :: non_neg_integer(),
          client_table = ets:new(table,
                                 [set,
                                  {keypos, 2},
                                  private]) :: ets:tid(),
          epoch = 0 :: non_neg_integer(),
          old_config :: [atom()],

          pending_replies = [] :: [],

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
    gen_server:call(Primary, {request, Command, self(), Request}, Timeout).

initial_config(Replica, Config) ->
    gen_server:call(Replica, {initial_config, Config}, infinity).

%% internal messages
prepare(Replica, View, Command, Op, Commit, Epoch) ->
    gen_server:cast(Replica, {prepare, self(), View,
                              Command, Op, Commit, Epoch}).

-record(prepare_ok, {view :: non_neg_integer(),
                     op :: non_neg_integer(),
                     epoch :: non_neg_integer(),
                     sender :: pid()}).
prepare_ok(Replica, View, Op, Epoch) ->
    gen_server:cast(Replica, #prepare_ok{view = View, op = Op,
                                         sender = self(),
                                         epoch = Epoch}).

commit(Replica, View, Commit, Epoch) ->
    gen_server:cast(Replica, {commit, View, Commit, Epoch}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, ModArgs, New, _Opts]) ->
    case New of
        true ->
            {ok, NextState, ModState} = Mod:init(ModArgs),
            {ok, ?S{mod = Mod,
                    next_state = NextState,
                    mod_state = ModState
                   }};
        false ->
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
                    [vrrm_replica:prepare(Replica, View,
                                          Command, Op1, Commit, Epoch)
                     || Replica <- Config, Replica /= self()],
                    {noreply, State?S{op = Op1}}
            end
    end;
handle_call({initial_config, Config}, _From, State) ->
    %% should check that we're in unconfigured+config=undefined
    [Primary | _] = Config,
    AmPrimary = self() =:= Primary,
    lager:info("~p inserting initial configuration, primary: ~p, ~p",
               [self(), Primary, AmPrimary]),
    Timeout =
        case AmPrimary of
            true ->
                2500;
            false ->
                5000
        end,
    {reply, ok, State?S{config = Config,
                        status=normal,
                        primary=AmPrimary},
     Timeout};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

%%% for all of the following message classes, if ViewNum < view_num,
%%% the message is ignored, or Epoch < epoch_num
handle_cast({prepare, Primary, View, Command, Op, Commit, Epoch} = Msg,
            State0) ->
    lager:info("~p prepare: command ~p", [self(), Msg]),
    %% is log complete? consider state transfer if not
    %% if CommitNum is higher than commit_num, do some upcalls
    State = maybe_catchup(Commit, State0),
    %% increment op number, append operation to log, update client table(?)
    Entry = #operation{num = Op,
                       view = View,
                       command = Command},
    _ = add_log_entry(Entry, State?S.log),
    %% reply with prepare_ok
    vrrm_replica:prepare_ok(Primary, View, Op, Epoch),
    %% reset primary failure timeout
    {noreply, State?S{op = Op}, timer:seconds(5)};
handle_cast(#prepare_ok{view = View, op = Op, epoch = Epoch},
            ?S{commit = Commit,
               view = LocalView,
               epoch = LocalEpoch} = State)
  when Commit >= Op; LocalEpoch > Epoch; LocalView > View ->
    %% ignore these, we've already processed them.
    {noreply, State};
handle_cast(#prepare_ok{view = View, op = Op, sender = Sender} = Msg,
            ?S{mod = Mod, next_state = NextState, mod_state = ModState,
               client_table = Table, log = Log,
               pending_replies = Pending} = State) ->
    %% do we have f replies? if no, wait for more (or timeout)
    F = f(State?S.config),
    OKs = scan_pend(prepare_ok, Op, View, Pending),
    lager:debug("~p prepare_ok: ~p, ~p N ~p F ~p",
               [self(), Msg, Pending, OKs, F]),
    case length(OKs ++ [Msg]) of
        N when N >= F ->
            lager:debug("got enough, replying"),
            %% update commit_num to OpNum, do Mod upcall, send {reply,
            %% v, s, x} set commit message timeout.
            #operation{command = Command, client = Client} =
                         get_log_entry(Op, View, Log),
            {reply, Reply, NextState1, ModState1} =
                Mod:NextState(Command, ModState),
            CliRec = get_client(Client, Table),
            ReqRec = get_request(CliRec, Op, View),
            lager:info("cr = ~p req = ~p", [CliRec, ReqRec]),
            gen_server:reply(ReqRec#request.from, {ok, Reply}),
            _ = add_reply(Client, ReqRec#request.req_num, Reply, Table),
            Pending1 = clean_pend(prepare_ok, Op, View, Pending),
            %% do we need to check if commit == Op - 1?
            {noreply, State?S{commit = Op,
                              mod_state = ModState1, next_state = NextState1,
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
    %% reset primary failure timeout, which should be configurable
    {noreply, State, timer:seconds(5)};
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

handle_info(timeout, ?S{primary = true} = State) ->
    %% send commit message to all replicas
    {noreply, State};
handle_info(timeout, ?S{primary = false} = State) ->
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

get_request(#client{requests = Requests} = _C, Op, View) ->
    lager:info("get_request c ~p o ~p v ~p", [_C, Op, View]),
    Req = [R || {_, #request{op = O, view = V} = R}
                    <- maps:to_list(Requests),
                O == Op andalso V == View],
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
    ets:insert(Log, Entry).

get_log_entry(Op, _View, Log) ->
    case ets:lookup(Log, Op) of
        [OpRecord] ->
            OpRecord;
        _ -> error(noes)
    end.

maybe_catchup(Commit, ?S{commit = LocalCommit} = State)
  when LocalCommit >= Commit ->
    State;
maybe_catchup(Commit, ?S{mod = Mod, next_state = NextState,
                         mod_state = ModState, log = Log,
                         view = View, commit = LocalCommit} = State) ->
    {NextState1, ModState1} =
        lists:foldl(fun(Op, {NState, MState}) ->
                            Entry = get_log_entry(Op, View, Log),
                            Cmd = Entry#operation.command,
                            case Mod:NState(Cmd, MState) of
                                {reply, _, NState1, MState1} ->
                                    {NState1, MState1};
                                {next_state, NState1, MState1} ->
                                    {NState1, MState1}
                            end
                    end,
                    {NextState, ModState},
                    lists:seq(LocalCommit + 1, Commit)),
    State?S{next_state = NextState1, mod_state = ModState1,
            commit = Commit}.

f(Config) ->
    L = length(Config),
    (L-1) div 2.

%% manually unroll this because the compiler is not smart
scan_pend(prepare_ok, Op, View, Pending) ->
    [R || #prepare_ok{view = V, op = O} = R <- Pending,
          O == Op, V == View].

clean_pend(prepare_ok, Op, View, Pending) ->
    [R || #prepare_ok{view = V, op = O} = R <- Pending,
          O == Op, V == View];
clean_pend(_Msg, _Op, _View, _Pending) ->
    error(unimplemented).
