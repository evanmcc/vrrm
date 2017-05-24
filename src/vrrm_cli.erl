-module(vrrm_cli).

-export([
         new/0,
         primary/0, primary/1,
         request/2, request/3,
         reconfigure/2, reconfigure/3
        ]).

-record(cli,
        {
          request = 0 :: non_neg_integer(),
          epoch = 0 :: non_neg_integer(),
          config = [] :: [pid()]
        }).

-opaque cli() :: #cli{}.

-export_type([cli/0]).

new() ->
    #cli{}.

primary() ->
    case get('$vrrm_cli_state') of
        undefined ->
            {error, no_active_client};
        C ->
            primary(C)
        end.

primary(#cli{config = [Primary|_]}) ->
    Primary.

%% two-arity form uses the pdict for lighter-weight use (silly
%% state-using protocol).
request(undefined, _Request) ->
    {error, cannot_find_node};
request(Replica, Request) when is_atom(Replica) ->
    %% will this loop when the local replica is perma-dead?
    lager:info("a thing"),
    request(whereis(Replica), Request);
request(Replica, Request) when is_pid(Replica) ->
    Cli =
        case get('$vrrm_cli_state') of
            undefined ->
                #cli{};
            C ->
                C
        end,
    case request(Replica, Request, Cli) of
        %% for convenience
        {ok, ok, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            ok;
        {ok, Reply, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            {ok, Reply};
        {error, Reason, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            {error, Reason}
    end.

request(undefined, _Request, Client) ->
    {error, cannot_find_node, Client};
request(Replica, Request, Client) when is_atom(Replica) ->
    request(whereis(Replica), Request, Client);
request(Replica, Request,
        #cli{config = []} = Client) ->
    Config = vrrm_replica:get_config(Replica),
    request(Replica, Request, Client#cli{config = Config});
request(Replica, Request,
        #cli{request = Next, epoch = Epoch,
             config = Config} = Client) ->
    lager:info("cli making request ~p to ~p", [Request, Replica]),
    case vrrm_replica:request(Replica, Request, Next) of
        {ok, Reply, ClusterEpoch} ->
            case ClusterEpoch of
                Epoch ->
                    {ok, Reply, Client#cli{request = Next+1}};
                CE when CE > Epoch ->
                    %% if the epoch has changed we need to refetch the
                    %% config.  this case covers epoch change that
                    %% leaves the primary in place.
                    {ok, Config} = vrrm_replica:get_config(Replica),
                    {ok, Reply, Client#cli{request = Next+1, epoch = ClusterEpoch,
                                           config = Config}}
            end;
        %% if we get this, a lot of things could have happened.
        %% network weather, dropped message, view change, epoch change.
        {error, timeout} ->
            %% retry on all nodes in the config (in reverse order, so
            %% we don't waste time immediately retrying a failed node
            %% another case where it'd ideally be async
            Resps = [vrrm_replica:request(R, Request, Next)
                     || R <- lists:reverse(Config)],
            %% this is potentially racy?
            case lists:keyfind(ok, 1, Resps) of
                {ok, Reply, Epoch1} ->
                    {ok, Reply, Client#cli{epoch = Epoch1}};
                false ->
                    %% no successful replies
                    %% TODO: punting on correct behavior here
                    {error, lost_cluster, Client}
            end;
        {error, not_primary, Primary} ->
            request(Primary, Request, Client)
    end.

reconfigure(Replica, Request) when is_pid(Replica) ->
    Cli =
        case get('$vrrm_cli_state') of
            undefined ->
                #cli{};
            C ->
                C
        end,
    case reconfigure(Replica, Request, Cli) of
        %% for convenience
        {ok, ok, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            ok;
        {ok, Reply, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            {ok, Reply};
        {error, Reason, Cli1} ->
            put('$vrrm_cli_state', Cli1),
            {error, Reason}
    end.

reconfigure(Replica, Request,
        #cli{config = []} = Client) ->
    Config = vrrm_replica:get_config(Replica),
    reconfigure(Replica, Request, Client#cli{config = Config});
reconfigure(Replica, Request,
            #cli{request = Next, epoch = Epoch,
                 config = Config} = Client) ->
    case vrrm_replica:reconfigure(Replica, Request, Next) of
        {ok, Reply, ClusterEpoch} ->
            case ClusterEpoch of
                Epoch ->
                    {ok, Reply, Client#cli{request = Next+1}};
                CE when CE > Epoch ->
                    %% if the epoch has changed we need to refetch the
                    %% config.  this case covers epoch change that
                    %% leaves the primary in place.
                    {ok, Config} = vrrm_replica:get_config(Replica),
                    {ok, Reply, Client#cli{request = Next+1, epoch = ClusterEpoch,
                                           config = Config}}
            end;
        %% if we get this, a lot of things could have happened.
        %% network weather, dropped message, view change, epoch change.
        {error, timeout} ->
            {error, primary_timeout};
        {error, not_primary, Primary} ->
            {error, new_primary, Primary}
    end.
