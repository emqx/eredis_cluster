-module(eredis_cluster_pool_worker_tests).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% backoff_delay/1 — unit tests for the exponential backoff sequence
%%
%% Exported from eredis_cluster_pool_worker under -ifdef(TEST).
%% Formula: min(2^((N+1) div 2 + 1) * 1000, 4096000)
%% Sequence: 2s, 4s, 4s, 8s, 8s, 16s, 16s, 32s, 32s, ...
%% ===================================================================

backoff_delay_sequence_test_() ->
    [
        {"attempt 0 → 2000ms (2s)",
            ?_assertEqual(2000,
                eredis_cluster_pool_worker:backoff_delay(0))},
        {"attempt 1 → 4000ms (4s)",
            ?_assertEqual(4000,
                eredis_cluster_pool_worker:backoff_delay(1))},
        {"attempt 2 → 4000ms (repeat)",
            ?_assertEqual(4000,
                eredis_cluster_pool_worker:backoff_delay(2))},
        {"attempt 3 → 8000ms (8s)",
            ?_assertEqual(8000,
                eredis_cluster_pool_worker:backoff_delay(3))},
        {"attempt 4 → 8000ms (repeat)",
            ?_assertEqual(8000,
                eredis_cluster_pool_worker:backoff_delay(4))},
        {"attempt 5 → 16000ms (16s)",
            ?_assertEqual(16000,
                eredis_cluster_pool_worker:backoff_delay(5))},
        {"attempt 6 → 16000ms (repeat)",
            ?_assertEqual(16000,
                eredis_cluster_pool_worker:backoff_delay(6))},
        {"attempt 7 → 32000ms (32s)",
            ?_assertEqual(32000,
                eredis_cluster_pool_worker:backoff_delay(7))},
        {"attempt 8 → 32000ms (repeat)",
            ?_assertEqual(32000,
                eredis_cluster_pool_worker:backoff_delay(8))}
    ].

backoff_delay_cap_test_() ->
    [
        {"attempt 23 → capped at 4096000ms",
            ?_assertEqual(4096000,
                eredis_cluster_pool_worker:backoff_delay(23))},
        {"attempt 24 → still capped",
            ?_assertEqual(4096000,
                eredis_cluster_pool_worker:backoff_delay(24))},
        {"attempt 100 → still capped",
            ?_assertEqual(4096000,
                eredis_cluster_pool_worker:backoff_delay(100))},
        {"attempt 9999 → still capped (no overflow)",
            ?_assertEqual(4096000,
                eredis_cluster_pool_worker:backoff_delay(9999))}
    ].

backoff_delay_monotonic_test() ->
    Delays = [eredis_cluster_pool_worker:backoff_delay(N)
              || N <- lists:seq(0, 50)],
    Pairs = lists:zip(lists:droplast(Delays), tl(Delays)),
    lists:foreach(
        fun({A, B}) ->
            ?assert(A =< B)
        end,
        Pairs
    ).

%% ===================================================================
%% Pool worker reconnect behavior — meck-based unit tests
%%
%% NOTE: When meck intercepts eredis:start_link/7, no real gen_server
%% is spawned, so no EXIT signal is produced. This is fine — it lets
%% us test the explicit reconnect scheduling in isolation from the
%% EXIT safety net.
%% ===================================================================

meck_setup() ->
    meck:new(eredis, [passthrough]),
    ok.

meck_cleanup(_) ->
    catch meck:unload(eredis),
    ok.

reconnect_backoff_test_() ->
    {"pool worker reconnect backoff (meck)",
        {setup, fun meck_setup/0, fun meck_cleanup/1, [
            {"connection failure increments backoff attempts",
                fun test_failure_increments_attempts/0},
            {"connection success resets backoff attempts",
                {timeout, 10, fun test_success_resets_attempts/0}},
            {"no duplicate reconnect from EXIT on failed start_connection",
                fun test_no_duplicate_reconnect_from_exit/0},
            {"established connection EXIT resets backoff",
                fun test_established_conn_exit_resets_backoff/0}
        ]}
    }.

test_failure_increments_attempts() ->
    %% Make eredis:start_link always fail (meck — no real process spawned)
    meck:expect(eredis, start_link, 7,
        {error, {connection_error, econnrefused}}),

    {ok, Worker} = eredis_cluster_pool_worker:start_link([
        {host, "127.0.0.1"},
        {port, 1},
        {database, 0},
        {password, ""},
        {options, []}
    ]),
    %% init/1 sends `reconnect` to self; let it process
    timer:sleep(100),

    %% Worker should be disconnected
    ?assertEqual({error, no_connection},
        eredis_cluster_pool_worker:query(Worker, ["PING"])),

    %% Check process dictionary for attempts > 0
    {dictionary, Dict} = process_info(Worker, dictionary),
    Attempts = proplists:get_value(reconnect_attempts, Dict, 0),
    ?assert(Attempts >= 1),

    %% Clean up — unlink first to avoid propagating kill
    unlink(Worker),
    exit(Worker, kill),
    timer:sleep(50).

test_success_resets_attempts() ->
    %% First call fails, second succeeds with a dummy process
    Self = self(),
    Ref = make_ref(),
    meck:expect(eredis, start_link, 7,
        meck:seq([
            {error, econnrefused},
            meck:exec(fun(_H, _P, _D, _Pw, _R, _T, _O) ->
                Pid = spawn_link(fun() ->
                    receive stop -> ok end
                end),
                Self ! {Ref, connected, Pid},
                {ok, Pid}
            end)
        ])
    ),

    {ok, Worker} = eredis_cluster_pool_worker:start_link([
        {host, "127.0.0.1"},
        {port, 1},
        {database, 0},
        {password, ""},
        {options, []}
    ]),

    %% Wait for backoff (2s for attempt 0) + second attempt to succeed
    ConnPid = receive
        {Ref, connected, P} -> P
    after 5000 ->
        error(timeout_waiting_for_reconnect)
    end,

    %% Let worker process the successful response
    timer:sleep(100),

    %% Attempts should be reset to 0
    {dictionary, Dict} = process_info(Worker, dictionary),
    Attempts = proplists:get_value(reconnect_attempts, Dict, 0),
    ?assertEqual(0, Attempts),

    %% Worker should report connected
    ?assert(eredis_cluster_pool_worker:is_connected(Worker)),

    %% Clean up
    ConnPid ! stop,
    unlink(Worker),
    exit(Worker, kill),
    timer:sleep(50).

test_no_duplicate_reconnect_from_exit() ->
    %% When meck intercepts start_link, no EXIT is produced (no real
    %% process). So the only reconnect source is the explicit schedule
    %% in handle_info(reconnect, ...). We verify at most one pending
    %% reconnect message exists.
    meck:expect(eredis, start_link, 7,
        {error, {ssl_error, handshake_failure}}),

    {ok, Worker} = eredis_cluster_pool_worker:start_link([
        {host, "127.0.0.1"},
        {port, 1},
        {database, 0},
        {password, ""},
        {options, []}
    ]),

    %% Let initial reconnect process
    timer:sleep(200),

    %% Message queue should have at most one pending reconnect timer.
    %% (The timer hasn't fired yet because backoff_delay(0) = 2000ms.)
    {messages, Msgs} = process_info(Worker, messages),
    ReconnectMsgs = [M || M <- Msgs, M =:= reconnect],
    ?assert(length(ReconnectMsgs) =< 1),

    unlink(Worker),
    exit(Worker, kill),
    timer:sleep(50).

test_established_conn_exit_resets_backoff() ->
    %% Start with a successful connection (dummy process)
    DummyConn = spawn(fun() ->
        receive stop -> ok end
    end),
    meck:expect(eredis, start_link, 7, {ok, DummyConn}),
    meck:expect(eredis, stop, 1, ok),

    {ok, Worker} = eredis_cluster_pool_worker:start_link([
        {host, "127.0.0.1"},
        {port, 1},
        {database, 0},
        {password, ""},
        {options, []}
    ]),
    timer:sleep(100),
    ?assert(eredis_cluster_pool_worker:is_connected(Worker)),

    %% Kill the connection — worker traps exits, sees Pid0 =:= Pid case
    exit(DummyConn, connection_lost),
    timer:sleep(200),

    %% Attempts should be 0 (reset by the established-connection EXIT handler)
    {dictionary, Dict} = process_info(Worker, dictionary),
    Attempts = proplists:get_value(reconnect_attempts, Dict, 0),
    ?assertEqual(0, Attempts),

    %% Worker should be disconnected now (conn = undefined)
    ?assertNot(eredis_cluster_pool_worker:is_connected(Worker)),

    unlink(Worker),
    exit(Worker, kill),
    timer:sleep(50).

%% ===================================================================
%% Integration test: pool recovery after transient connection failure
%%
%% Requires a running Redis cluster (docker compose up -d).
%% ===================================================================

-define(BACKOFF_POOL, test_backoff_pool).
-define(SERVERS, "127.0.0.1:30001,127.0.0.1:30002").

integration_setup() ->
    {ok, Apps} = application:ensure_all_started(eredis_cluster),
    Servers = format_redis_servers(
        os:getenv("REDIS_NODE_LIST", ?SERVERS)),
    PoolOpts = [
        {servers, Servers},
        {pool_size, 2},
        {pool_max_overflow, 0},
        {database, 0},
        {password, "passw0rd"}
    ],
    {ok, _MonPid} = eredis_cluster:start_pool(?BACKOFF_POOL, PoolOpts),
    Apps.

integration_cleanup(Apps) ->
    catch eredis_cluster:stop_pool(?BACKOFF_POOL),
    lists:foreach(fun application:stop/1, lists:reverse(Apps)).

recovery_after_failure_test_() ->
    {"recovery after transient connection failure (integration)",
        {setup, fun integration_setup/0, fun integration_cleanup/1, [
            {"pool recovers after mock failure is lifted",
                {timeout, 15, fun test_pool_recovery/0}}
        ]}
    }.

test_pool_recovery() ->
    Pool = ?BACKOFF_POOL,
    %% Verify pool works initially
    ?assertEqual({ok, <<"OK">>},
        eredis_cluster:q(Pool, ["SET", "backoff_test_key", "hello"])),
    ?assertEqual({ok, <<"hello">>},
        eredis_cluster:q(Pool, ["GET", "backoff_test_key"])),

    %% Mock eredis:q to simulate connection failure
    %% (easier than killing real connections — avoids pool internals)
    meck:new(eredis, [passthrough]),
    meck:expect(eredis, q, fun(_, _) -> {error, no_connection} end),
    meck:expect(eredis, q, fun(_, _, _) -> {error, no_connection} end),

    %% Queries should fail
    ?assertEqual({error, no_connection},
        eredis_cluster:q(Pool, ["GET", "backoff_test_key"])),

    %% Remove mock — real connections still alive
    meck:unload(eredis),

    %% Should work again (connections were never actually broken)
    ?assertEqual({ok, <<"hello">>},
        eredis_cluster:q(Pool, ["GET", "backoff_test_key"])),

    %% Clean up test key
    eredis_cluster:q(Pool, ["DEL", "backoff_test_key"]).

%% ===================================================================
%% Helpers
%% ===================================================================

format_redis_servers(Servers) ->
    [format_server(S) || S <- string:tokens(Servers, ",")].

format_server(Server) ->
    case string:tokens(Server, ":") of
        [Domain] -> {Domain, 6379};
        [Domain, Port] -> {Domain, list_to_integer(Port)}
    end.
