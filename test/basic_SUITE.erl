-module(basic_SUITE).

-compile(export_all).

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    ok = lager:start(),
    ok = application:start(vrrm),
    application:set_env(vrrm, idle_commit_interval, 250),
    application:set_env(vrrm, primary_failure_interval, 400),

    lager_common_test_backend:bounce(info),
    Config.

end_per_suite(_Config) ->
    %% poorly behaved tests will leak processes here, we should expend
    %% some effort to find them and shut them down
    ok = application:stop(vrrm),
    ok.

groups() ->
    [
     {cluster,
      [shuffle],
      [
       blackboard
      ]}
    ].

all() ->
    [{group, cluster}].

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

blackboard(Config) ->
    %% normally we would run this by starting the supervisor, so
    %% eventually this should be easier

    Replicas0 =
        [begin
             {ok, R} =
                 vrrm_replica:start_link(vrrm_blackboard,
                                         [], true, #{}),
             R
         end
         || _ <- lists:seq(1, 3)],

    Replicas = lists:sort(Replicas0),
    [vrrm_replica:initial_config(R, Replicas)
     || R <- Replicas],

    [Primary|Rest] = Replicas,
    lager:info("replicas: ~p primary; ~p", [Replicas, Primary]),

    %% need to abstract some of this away in a client module

    %% set initial state
    {ok, ok} = vrrm_replica:request(Primary, {put, foo, bar}, 1),
    %% test idempotency of requests
    {ok, ok} = vrrm_replica:request(Primary, {put, foo, bar}, 1),
    %% check here that op/commit are the same as before

    {ok, bar} = vrrm_replica:request(Primary, {get, foo}, 2),

    lager:info("suspending primary"),

    %% kill the primary
    sys:suspend(Primary),
    timer:sleep(1000), % lower the timeouts?

    %% make a request to all others and make sure that it works
    Replies = [vrrm_replica:request(R, {get, foo}, 3)
               || R <- Rest],

    lager:info("replies: ~p", [Replies]),
    %% compare all the module state to make sure that it matches

    [Primary2|_] = Rest,
    {ok, ok} = vrrm_replica:request(Primary2, {put, baz, quux}, 4),

    lager:info("resuming primary"),
    %% allow the primary to recover
    sys:resume(Primary),

    lager:info("validating primary recovery"),
    %% this is bad(?), request takes the place of a sleep.
    {error, timeout} = vrrm_replica:request(Primary, {get, baz}, 5),
    {ok, quux} = vrrm_replica:request(Primary, {get, baz}, 5),

    %% make two new nodes and join them to the cluster
    NewNodes =
        [begin
             {ok, R} =
                 vrrm_replica:start_link(vrrm_blackboard,
                                         [], true, #{}),
             R
         end
         || _ <- lists:seq(1, 3)],
    %% add three new nodes, with the primary exiting and leaving
    NewConfig0 = (Replicas ++ NewNodes) -- [Primary],
    NewConfig = lists:sort(NewConfig0),
    [NewPrimary|_] = NewConfig,
    lager:info("starting reconfigure"),
    {ok, ok} = vrrm_replica:reconfigure(Primary, NewConfig, 6),

    timer:sleep(200),

    ModState0 =
        [begin
             S = vrrm_replica:get_mod_state(R),
             lager:debug("state ~p", [S]),
             S
         end
         || R <- NewConfig],
    ModState = lists:usort(ModState0),
    lager:info("modstate ~p", [ModState]),
    1 = length(ModState),

    {ok, bar} = vrrm_replica:request(NewPrimary, {get, foo}, 7),

    {ok, ok} = vrrm_replica:request(NewPrimary, {put, foo, blort}, 8),
    {ok, blort} = vrrm_replica:request(NewPrimary, {get, foo}, 9),

    false = is_process_alive(Primary),

    %% eprof:start(),
    %% eprof:start_profiling(NewConfig),

    Ct = 100000,
    Time =
        timer:tc(fun() ->
                         _ = [vrrm_replica:request(NewPrimary,
                                                   {put, ctr, N},
                                                   9 + N)
                              || N <- lists:seq(1, Ct)]
                 end),

    {TimeNs, _} = Time,

    TimeSec = TimeNs div 1000000,
    {ok, Ct} = vrrm_replica:request(NewPrimary,
                                    {get, ctr},
                                    9 + Ct + 10),

    lager:warning("Time ~p in ~p", [Ct, TimeSec]),

    %% eprof:stop_profiling(),
    %% eprof:analyze(total),

    Config.
