-module(vrrm).

%% API exports
-export([
         config/1, config/2
        ]).

%%====================================================================
%% API functions
%%====================================================================

config(Key) ->
    {ok, Val} = application:get_env(vrrm, Key),
    Val.

config(Key, Default) ->
    application:get_env(vrrm, Key, Default).

%%====================================================================
%% Internal functions
%%====================================================================
