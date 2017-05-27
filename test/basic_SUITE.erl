-module(basic_SUITE).

-compile(export_all).

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    %% application:load(lager),
    %% application:set_env(lager, handlers,
    %%                     [{lager_console_backend,
    %%                       [debug,
    %%                        {lager_default_formatter, [date, " ", time, " ",
    %%                                                   color, "[", severity, "] ",
    %%                                                   pid, " ",
    %%                                                   {module, [
    %%                                                             "", module, ":",
    %%                                                             {function, ["", function, ":"], ""},
    %%                                                             {line, ["",line], ""}], ""},
    %%                                                   " ", message, "\n"]}]}]),
    lager:start(),
    ok = application:start(vrrm),
    application:set_env(vrrm, idle_commit_interval, 250),
    application:set_env(vrrm, primary_failure_interval, 400),
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
    %% have to reverse because of initial view change and synchrony
    %% problems, need a better solution later for initial view stuff
    [vrrm_replica:initial_config(R, Replicas)
     || R <- lists:reverse(Replicas)],

    [Primary|Rest] = Replicas,
    lager:info("replicas: ~p primary; ~p", [Replicas, Primary]),

    %% need to abstract some of this away in a client module

    %% set initial state
    ok = vrrm_cli:request(Primary, {put, foo, bar}),
    %% test idempotency of requests
    {ok, ok, 0} = vrrm_replica:request(Primary, {put, foo, bar}, 1),
    %% check here that op/commit are the same as before

    {ok, bar} = vrrm_cli:request(Primary, {get, foo}),

    lager:info("suspending primary"),

    %% kill the primary
    sys:suspend(Primary),
    timer:sleep(1000), % lower the timeouts?

    {ok, bar} = vrrm_cli:request(Primary, {get, foo}),

    %% lager:info("replies: ~p", [Replies]),
    %% compare all the module state to make sure that it matches

    [Primary2|_] = Rest,
    ok = vrrm_cli:request(Primary2, {put, baz, quux}),

    lager:info("resuming primary"),
    %% allow the primary to recover
    sys:resume(Primary),

    lager:info("validating primary recovery"),

    %% should validate here that this request takes $Timeout ms
    {ok, quux} = vrrm_cli:request(Primary, {get, baz}),

    %% make three new nodes and join them to the cluster
    NewNodes =
        [begin
             {ok, R} =
                 vrrm_replica:start_link(vrrm_blackboard,
                                         [], true, #{}),
             R
         end
         || _ <- lists:seq(1, 3)],
    %% add three new nodes, with the first exiting and leaving
    NewConfig0 = (Replicas ++ NewNodes) -- [Primary],
    NewConfig = lists:sort(NewConfig0),
    [NewPrimary|_] = NewConfig,
    lager:info("starting reconfigure"),
    case vrrm_cli:reconfigure(Primary, NewConfig) of
        ok -> ok;
        {error, _, NewPrimary} ->
            vrrm_cli:reconfigure(NewPrimary, NewConfig)
    end,

    timer:sleep(300),

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

    {ok, bar} = vrrm_cli:request(NewPrimary, {get, foo}),

    ok = vrrm_cli:request(NewPrimary, {put, foo, blort}),
    {ok, blort} = vrrm_cli:request(NewPrimary, {get, foo}),

    true =
        (fun Loop(0) ->
                 {error, too_long};
             Loop(N) ->
                 case is_process_alive(Primary) of
                     true ->
                         timer:sleep(50),
                         Loop(N - 1);
                     false ->
                         true
                 end
         end)(20 * 10),

    %% eprof:start(),
    %% eprof:start_profiling(NewConfig),

    Ct = 100000,
    Time =
        timer:tc(fun() ->
                         _ = [vrrm_cli:request(NewPrimary,
                                                   {put, ctr, N})
                              || N <- lists:seq(1, Ct)]
                 end),

    {TimeNs, _} = Time,

    TimeSec = TimeNs div 1000000,
    {ok, Ct} = vrrm_cli:request(NewPrimary, {get, ctr}),

    lager:warning("Time ~p in ~p", [Ct, TimeSec]),

    %% eprof:stop_profiling(),
    %% eprof:analyze(total),

    Config.
