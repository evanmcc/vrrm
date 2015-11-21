-module(basic_SUITE).

-compile(export_all).

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
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
       create,
       blackboard
      ]}
    ].

all() ->
    [{group, cluster}].

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

create(Config) ->
    Config.

blackboard(Config) ->
    Config.
