%%%-------------------------------------------------------------------
%%% @author Evan Vigil-McClanahan <mcclanhan@gmail.com>
%%% @copyright (C) 2017, Evan Vigil-McClanahan
%%% @doc
%%%
%%% @end
%%% Created : 21 May 2017 by Evan Vigil-McClanahan <mcclanhan@gmail.com>
%%%-------------------------------------------------------------------
-module(vrrm_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->

    SupFlags = #{strategy => one_for_one,
                 intensity => 3,
                 period => 5000},

    Child = #{id => vrrm_manager,
              start => {vrrm_manager, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker,
              modules => [vrrm_manager]},

    {ok, {SupFlags, [Child]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
