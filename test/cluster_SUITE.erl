-module(cluster_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    lager_common_test_backend:bounce(info),
    %% make a release as test, should likely use better exit status
    %% stuff rather than assuming that it passes
    %% lager:info("~p", [os:cmd("pwd")]),
    BR = os:cmd("(cd ../../../..; rebar3 as test release)"),
    lager:info("BR: ~p", [BR]),

    %% start several nodes
    Nodes =
        [begin
             N = integer_to_list(N0),
             Name = "test_sup_" ++ N ++ "@127.0.0.1",
             Env = [{"RELX_REPLACE_OS_VARS", "true"},
                    {"NODE_NAME", Name}],
             Cmd = "../../rel/test_sup/bin/test_sup console",
             Pid = cmd(Cmd, Env),
             lager:info("cmd ~p ~p", [Cmd, Env]),
             %%timer:sleep(1000),
             {Pid, list_to_atom(Name)}
         end
         || N0 <- lists:seq(1, 5)],

    %% Collect pids here (actually, using console + cmd() we don't
    %% need to, because console will exit when the testrunner exits).
    lager:debug("ps ~p", [os:cmd("ps aux | grep beam.sm[p]")]),

    %% establish disterl connections to each of them
    NodeName = 'testrunner@127.0.0.1',
    net_kernel:start([NodeName]),
    erlang:set_cookie(node(), test_sup_cookie),
    [begin
         lager:debug("attaching to ~p", [Node]),
         connect(Node, 50, 40)
     end
     || {_Pid, Node} <- Nodes],
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    %% poorly behaved tests will leak processes here, we should expend
    %% some effort to find them and shut them down
    Nodes = ?config(nodes, Config),
    [begin
         os:putenv("NODE_NAME", atom_to_list(Node)),
         os:cmd("test/rel/test_sup/bin/test_sup stop")
     end
     || {_Pid, Node} <- Nodes],
    ok.

init_per_group(_, Config) ->
    Config.

connect(Node, _Wait, 0) ->
    lager:error("could not connect to ~p, exiting", [Node]),
    exit(disterl);
connect(Node, Wait, Tries) ->
    try
        true = net_kernel:hidden_connect_node(Node),
        pong = net_adm:ping(Node)
    catch _:_ ->
            lager:debug("connect failed: ~p ~p", [Node, Tries]),
            timer:sleep(Wait),
            connect(Node, Wait, Tries - 1)
    end.

end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

groups() ->
    [
     {init,
      [],
      [
       bootstrap
      ]},
     {operations,
      [],
      [
       start_predefined
       %% start_dynamic
       %% terminate_dynamic
       %% kill_random
      ]}

    ].

all() ->
    [
     {group, init},
     {group, operations}
    ].

bootstrap(Config) ->
    [{_, Lead}|Rest] = _Nodes = ?config(nodes, Config),
    [begin
         R = rpc:call(Node, test_sup, join, [Lead]),
         lager:debug("ret ~p", [R]),
         ok = R
     end
     || {_, Node} <- Rest],
    timer:sleep(100),
    [begin
         R = rpc:call(Node, test_sup, start, [whereis(lager_event)]),
         lager:info("ret ~p", [R]),
         {ok, _} = R
     end
     || {_, Node} <- [{1, Lead}]], %% Nodes],
    Entry = rpc:call(Lead, erlang, whereis, [vrrm_ha_sup]),
    Reply = vrrm_cli:request(Entry, which_children),
    lager:info("reply ~p", [Reply]),
    Config.

start_predefined(Config) ->
    Config.

%% util

cmd(Cmd, Env) ->
    spawn(fun() ->
                  P = open_port({spawn, Cmd},
                                [{env, Env},
                                 stream, use_stdio,
                                 exit_status,
                                 stderr_to_stdout]),
                  loop(P)
          end).

loop(Port) ->
    receive
        stop ->
            ok;
        {Port, {data, Data}} ->
            lager:info("port data ~p ~p", [Port, Data]),
            loop(Port);
        {Port, {exit_status, Status}} ->
            case Status of
                0 ->
                    lager:debug("port exit ~p ~p", [Port, Status]);
                _ ->
                    lager:info("port exit ~p ~p", [Port, Status])
            end,
            ok
    end.
