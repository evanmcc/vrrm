-module(vrrm_app).

-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    %%{ok, _} = application:ensure_all_started(lager),
    vrrm_sup:start_link().

stop(_State) ->
    ok.
