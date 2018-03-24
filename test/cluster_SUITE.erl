-module(cluster_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    application:ensure_all_started(lager),
    application:load(vrrm),
    lager:start(),

    %% start several nodes
    ErlFlags =  "-config ../../../../test/config/sys.config",
        %%"-args_file ../../../../test/config/vm.args",
    ct:pal("path ~p", [os:cmd("pwd")]),
    CodePath = code:get_path(),
    Nodes =
        [begin
             N = integer_to_list(N0),
             Name = test_node(N),
             {ok, HostNode} = ct_slave:start(
                                Name,
                                [{kill_if_fail, true},
                                 {monitor_master, true},
                                 {init_timeout, 3000},
                                 {startup_timeout, 3000},
                                 {startup_functions,
                                  [{code, set_path, [CodePath]},
                                   {application, load, [lager]},
                                   {application, set_env,
                                    [lager, handlers,
                                     [{lager_file_backend,
                                       [{file, "console"++N++".log"}, {level, info}]}]]},
                                   {application, ensure_all_started, [vrrm]}]},
                                 {erl_flags, ErlFlags}]),
             HostNode
         end
         || N0 <- lists:seq(1, 5)],

    [begin
        true = net_kernel:hidden_connect_node(Node)%,
        %pong = net_adm:ping(Node)
     end
     || Node <- Nodes],
    [First | Rest] = Nodes,
    [rpc:call(Node, net_adm, ping, [First])
     || Node <- Rest],
    [{nodes, Nodes} | Config].

hostname() ->
    string:strip(os:cmd("hostname"), right, $\n).

test_node(N) ->
    list_to_atom("test_sup_" ++ N ++ "@127.0.0.1").

end_per_suite(Config) ->
    %% poorly behaved tests will leak processes here, we should expend
    %% some effort to find them and shut them down
    Nodes = ?config(nodes, Config),
    [begin
         ct_slave:stop(Node)
     end
     || Node <- Nodes],
    ok.

init_per_group(_, Config) ->
    Config.

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
       %%start_predefined
       start_dynamic
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
    Nodes = ?config(nodes, Config),
    [First | _] = Nodes,
    %% allow all the nodes time to see each other
    timer:sleep(2000), % lower?
    R = rpc:call(First, vrrm_manager, start_quorum,
                 [test_quorum, vrrm_blackboard, [], all]),
    ?assertEqual(ok, R),
    ok = wait_till_healthy(First),
    Config.

start_dynamic(Config) ->
    Config.

%% util

wait_till_healthy(Node) ->
    wait_till_healthy(Node, 200). % 10 seconds

wait_till_healthy(_Node, 0) ->
    {error, took_too_long};
wait_till_healthy(Node, N) ->
    case rpc:call(Node, vrrm_manager, quorum_status, [test_quorum]) of
        normal ->
            ok;
        Other ->
            timer:sleep(50),
            ct:pal("waiting ~p ~p", [N, Other]),
            wait_till_healthy(Node, N - 1)
    end.
