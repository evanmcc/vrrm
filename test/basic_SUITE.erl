-module(basic_SUITE).

-compile(export_all).

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    ok = lager:start(),
    lager_common_test_backend:bounce(info),
    Config.

end_per_suite(_Config) ->
    %% poorly behaved tests will leak processes here, we should expend
    %% some effort to find them and shut them down
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

    [Primary|_] = Replicas,
    lager:info("replicas: ~p primary; ~p", [Replicas, Primary]),

    %% need to abstract some of this away in a client module

    %% set initial state
    {ok, ok} = vrrm_replica:request(Primary, {put, foo, bar}, 1),
    %% test idempotency of requests
    {ok, ok} = vrrm_replica:request(Primary, {put, foo, bar}, 1),
    {ok, bar} = vrrm_replica:request(Primary, {get, foo}, 2),

    Config.
