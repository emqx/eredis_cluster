-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/5]).
-export([create/6]).
-export([stop/1]).
-export([transaction/2]).
-export([do_transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

-type pool_name() :: binary().

create(Host, Port, DataBase, Credentials, PoolOptions) ->
    create(Host, Port, DataBase, Credentials, PoolOptions, []).
create(Host, Port, DataBase, Credentials, PoolOptions0, Options) ->
    PoolName = get_name(Host, Port, Options),
    WorkerArgs = [{host, Host},
                  {port, Port},
                  {database, DataBase},
                  {credentials, Credentials}],
    PoolOptions1 = case Options of
                      [] -> PoolOptions0 ++ WorkerArgs;
                      _ ->  PoolOptions0 ++ WorkerArgs ++ [{options, Options}]
                  end,
    case ecpool:start_sup_pool(PoolName, eredis_cluster_pool_worker, PoolOptions1) of
        {ok, _} ->
            {ok, PoolName};
        {error, {already_started, _Pid}} ->
            {ok, PoolName};
        {error, _} = Error ->
            Error
    end.

-spec transaction(PoolName::pool_name(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        ecpool:pick_and_do(PoolName, {?MODULE, do_transaction, [Transaction]}, handover)
    catch
        exit:_ ->
            {error, no_connection}
    end.

do_transaction(Pid, Transaction) ->
    Transaction(Pid).

-spec stop(PoolName::pool_name()) -> ok.
stop(PoolName) ->
    _ = ecpool:stop_sup_pool(PoolName),
    ok.

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.

get_name(Host, Port, Options) ->
    Prefix = proplists:get_value(worker_name_prefix, Options, rand_int()),
    list_to_binary(str(Prefix) ++ "_" ++ Host ++ "#" ++ str(Port)).

str(S) when is_integer(S) -> integer_to_list(S);
str(S) when is_binary(S) -> binary_to_list(S);
str(S) when is_atom(S) -> atom_to_list(S);
str(S) when is_list(S) -> S.

rand_int() ->
    <<R:32>> = crypto:strong_rand_bytes(4),
    R.
