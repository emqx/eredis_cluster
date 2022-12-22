-module(eredis_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

-export([transaction/2]).
-export([update_key/2]).
-export([update_hash_field/3]).

-define(POOL, ?MODULE).
-define(POOL_OPTS, [
    {servers, [
        {"127.0.0.1", 30001},
        {"127.0.0.1", 30002}
    ]},
    {pool_size, 5},
    {pool_max_overflow, 0},
    {database, 0},
    {password, "passw0rd"}
]).

setup() ->
    {ok, Apps} = application:ensure_all_started(eredis_cluster),
    {ok, _Pid} = eredis_cluster:start_pool(?POOL, ?POOL_OPTS),
    Apps.

cleanup(Apps) ->
    ok = eredis_cluster:stop_pool(?POOL),
    ok = lists:foreach(fun application:stop/1, lists:reverse(Apps)).

basic_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,
        [
            { "get and set",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?POOL, ["SET", "key", "value"])),
                ?assertEqual({ok, <<"value">>}, eredis_cluster:q(?POOL, ["GET","key"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(?POOL, ["GET","nonexists"]))
            end
            },

            { "binary",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?POOL, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
                ?assertEqual({ok, <<"value_binary">>}, eredis_cluster:q(?POOL, [<<"GET">>,<<"key_binary">>])),
                ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], eredis_cluster:qp(?POOL, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]]))
            end
            },

            { "delete test",
            fun() ->
                ?assertMatch({ok, _}, eredis_cluster:q(?POOL, ["DEL", "a"])),
                ?assertEqual({ok, <<"OK">>}, eredis_cluster:q(?POOL, ["SET", "b", "a"])),
                ?assertEqual({ok, <<"1">>}, eredis_cluster:q(?POOL, ["DEL", "b"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(?POOL, ["GET", "b"]))
            end
            },

            { "pipeline",
            fun () ->
                ?assertNotMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?POOL, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
                ?assertMatch([{ok, _},{ok, _},{ok, _}], eredis_cluster:qp(?POOL, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]]))
            end
            },

            { "transaction",
            fun () ->
                ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction(?POOL, [["get","abc"],["get","abc"],["get","abc"]])),
                ?assertMatch({error,_}, eredis_cluster:transaction(?POOL, [["get","abc"],["get","abcde"],["get","abcd1"]]))
            end
            },

            { "function transaction",
            fun () ->
                eredis_cluster:q(?POOL, ["SET", "efg", "12"]),
                Function = fun(Worker) ->
                    eredis_cluster:qw(Worker, ["WATCH", "efg"]),
                    {ok, Result} = eredis_cluster:qw(Worker, ["GET", "efg"]),
                    NewValue = binary_to_integer(Result) + 1,
                    timer:sleep(100),
                    lists:last(eredis_cluster:qw(Worker, [["MULTI"],["SET", "efg", NewValue],["EXEC"]]))
                end,
                PResult = rpc:pmap({?MODULE, transaction},["efg"],lists:duplicate(5, Function)),
                % PResult = ?MODULE:transaction(Function, "efg"),
                % ?assertEqual(ok, PResult),
                Nfailed = lists:foldr(fun({_, Result}, Acc) -> if Result == undefined -> Acc + 1; true -> Acc end end, 0, PResult),
                ?assertEqual(4, Nfailed)
            end
            },

            { "eval key",
            fun () ->
                eredis_cluster:q(?POOL, ["del", "foo"]),
                eredis_cluster:q(?POOL, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?POOL, ["GET", "foo"]))
            end
            },

            { "evalsha",
            fun () ->
                % In this test the key "load" will be used because the "script
                % load" command will be executed in the redis server containing
                % the "load" key. The script should be propagated to other redis
                % client but for some reason it is not done on Travis test
                % environment. @TODO : fix travis redis cluster configuration,
                % or give the possibility to run a command on an arbitrary
                % redis server (no slot derived from key name)
                eredis_cluster:q(?POOL, ["del", "load"]),
                {ok, Hash} = eredis_cluster:q(?POOL, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
                eredis_cluster:q(?POOL, ["evalsha", Hash, 1, "load"]),
                ?assertEqual({ok, <<"bar">>}, eredis_cluster:q(?POOL, ["GET", "load"]))
            end
            },

            { "bitstring support",
            fun () ->
                eredis_cluster:q(?POOL, [<<"set">>, <<"bitstring">>,<<"support">>]),
                ?assertEqual({ok, <<"support">>}, eredis_cluster:q(?POOL, [<<"GET">>, <<"bitstring">>]))
            end
            },

            { "flushdb",
            fun () ->
                eredis_cluster:q(?POOL, ["set", "zyx", "test"]),
                eredis_cluster:q(?POOL, ["set", "zyxw", "test"]),
                eredis_cluster:q(?POOL, ["set", "zyxwv", "test"]),
                eredis_cluster:flushdb(?POOL),
                ?assertEqual({ok, undefined}, eredis_cluster:q(?POOL, ["GET", "zyx"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(?POOL, ["GET", "zyxw"])),
                ?assertEqual({ok, undefined}, eredis_cluster:q(?POOL, ["GET", "zyxwv"]))
            end
            },

            { "atomic get set",
            fun () ->
                eredis_cluster:q(?POOL, ["set", "hij", 2]),
                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
                Result = rpc:pmap({?MODULE, update_key}, [Incr], lists:duplicate(5, "hij")),
                IntermediateValues = proplists:get_all_values(ok, Result),
                ?assertEqual([3,4,5,6,7], lists:sort(IntermediateValues)),
                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?POOL, ["get", "hij"]))
            end
            },

            { "atomic hget hset",
            fun () ->
                eredis_cluster:q(?POOL, ["hset", "klm", "nop", 2]),
                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
                Result = rpc:pmap({?MODULE, update_hash_field}, ["nop", Incr], lists:duplicate(5, "klm")),
                IntermediateValues = proplists:get_all_values(ok, Result),
                ?assertEqual([{<<"0">>,3},{<<"0">>,4},{<<"0">>,5},{<<"0">>,6},{<<"0">>,7}], lists:sort(IntermediateValues)),
                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(?POOL, ["hget", "klm", "nop"]))
            end
            },

            { "eval",
            fun () ->
                Script = <<"return redis.call('set', KEYS[1], ARGV[1]);">>,
                ScriptHash = << << if N >= 10 -> N -10 + $a; true -> N + $0 end >> || <<N:4>> <= crypto:hash(sha, Script) >>,
                eredis_cluster:eval(?POOL, Script, ScriptHash, ["qrs"], ["evaltest"]),
                ?assertEqual({ok, <<"evaltest">>}, eredis_cluster:q(?POOL, ["get", "qrs"]))
            end
            }

      ]
    }
}.

transaction(Transaction, PoolKey) ->
    eredis_cluster:transaction(?POOL, Transaction, PoolKey).

update_key(Key, UpdateFun) ->
    eredis_cluster:update_key(?POOL, Key, UpdateFun).

update_hash_field(Key, Field, UpdateFun) ->
    eredis_cluster:update_hash_field(?POOL, Key, Field, UpdateFun).
