-module(vrrm_app).

-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1]).


%% ifdef this out?
-export([swap_lager/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    vrrm_sup:start_link().

stop(_State) ->
    ok.

swap_lager(Pid) ->
    %% our testing environment has provided us with a remote
    %% lager sink to target messages at, but we can't target
    %% it directly, so proxy message through to it.
    Proxy = spawn(fun Loop() ->
                          receive
                              E -> Pid ! E
                          end,
                          Loop()
                  end),
    Lager = whereis(lager_event),
    true = unregister(lager_event),
    case register(lager_event, Proxy) of
        true ->
            lager:info("hey we just did the bad thing with ~p", [Pid]);
        Other ->
            register(lager_event, Lager),
            lager:info("noes we failed: ~p", [Other])
    end.
