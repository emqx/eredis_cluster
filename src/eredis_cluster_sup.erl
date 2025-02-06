-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0]).
-export([start_child/2, has_child/1, stop_child/1]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    ets:new(eredis_cluster_monitor, [public, set, named_table, {read_concurrency, true}]),
    Procs = [{eredis_cluster_pool,
                {eredis_cluster_pool, start_link, []},
                permanent, 5000, supervisor, [dynamic]}
            ],
    {ok, {{one_for_one, 1, 5}, Procs}}.

start_child(Name, Opts) ->
    ChildSpec = {name(Name),
                 {eredis_cluster_monitor, start_link, Opts},
                 permanent, 5000, worker, [eredis_cluster_monitor]},
    supervisor:start_child(?MODULE, ChildSpec).

stop_child(Name) ->
    case supervisor:terminate_child(?MODULE, name(Name)) of
        ok -> supervisor:delete_child(?MODULE, name(Name));
        Error -> Error
    end.

has_child(Name) ->
    case supervisor:get_childspec(?MODULE, name(Name)) of
        {ok, _} ->
            true;
        {error, not_found} ->
            false
    end.

name(Name) ->
    {eredis_cluster_monitor, Name}.
