-module(locker_SUITE).
-compile([export_all]).
-include_lib("test_server/include/test_server.hrl").
-include_lib("eqc/include/eqc.hrl").

all() ->
    [
     quorum,
     no_quorum_possible,
     lease_extend
    ].
%%     qc].

quorum(_) ->
    [A, B, C] = setup([a, b, c]),
    ok = rpc:call(A, locker, add_node, [B]),
    ok = rpc:call(A, locker, add_node, [C]),
    ok = rpc:call(B, locker, add_node, [C]),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {1, catch rpc:call(A, locker, lock, [123, Parent])}
          end),
    spawn(fun() ->
                  Parent ! {2, catch rpc:call(B, locker, lock, [123, Parent])}
          end),
    receive {1, P1} -> P1 after 1000 -> throw(timeout) end,
    receive {2, P2} -> P2 after 1000 -> throw(timeout) end,

    ?line {ok, Pid} = rpc:call(A, locker, pid, [123]),
    ?line {ok, Pid} = rpc:call(B, locker, pid, [123]),
    ?line {ok, Pid} = rpc:call(C, locker, pid, [123]),

    {ok, [], [{123, {Pid, _, _}}], _, _} = rpc:call(A, locker, get_debug_state, []),
    {ok, [], [{123, {Pid, _, _}}], _, _} = rpc:call(B, locker, get_debug_state, []),
    {ok, [], [{123, {Pid, _, _}}], _, _} = rpc:call(C, locker, get_debug_state, []),

    teardown([A, B, C]).

no_quorum_possible(_) ->
    [A, B, C] = setup([a, b, c]),
    ok = rpc:call(A, locker, add_node, [B]),
    ok = rpc:call(A, locker, add_node, [C]),
    ok = rpc:call(B, locker, add_node, [C]),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {1, catch rpc:call(A, locker, lock, [123, Parent])}
          end),
    spawn(fun() ->
                  Parent ! {2, catch rpc:call(B, locker, lock, [123, Parent])}
          end),
    spawn(fun() ->
                  Parent ! {3, catch rpc:call(C, locker, lock, [123, Parent])}
          end),
    receive {1, P1} -> P1 after 1000 -> throw(timeout) end,
    receive {2, P2} -> P2 after 1000 -> throw(timeout) end,
    receive {3, P3} -> P3 after 1000 -> throw(timeout) end,

    {error, not_found} = rpc:call(A, locker, pid, [123]),
    {error, not_found} = rpc:call(B, locker, pid, [123]),
    {error, not_found} = rpc:call(C, locker, pid, [123]),

    {ok, [], [], _, _} = rpc:call(A, locker, get_debug_state, []),
    {ok, [], [], _, _} = rpc:call(B, locker, get_debug_state, []),
    {ok, [], [], _, _} = rpc:call(C, locker, get_debug_state, []),

    teardown([A, B, C]).


lease_extend(_) ->
    [A, B, C] = setup([a, b, c]),
    ok = rpc:call(A, locker, add_node, [B]),
    ok = rpc:call(A, locker, add_node, [C]),
    ok = rpc:call(B, locker, add_node, [C]),

    Pid = self(),
    ok = rpc:call(A, locker, lock, [123, Pid]),
    {ok, Pid} = rpc:call(A, locker, pid, [123]),
    {ok, Pid} = rpc:call(B, locker, pid, [123]),
    {ok, Pid} = rpc:call(C, locker, pid, [123]),

    timer:sleep(2000),
    rpc:sbcast([A, B, C], locker, expire_leases),

    {error, not_found} = rpc:call(A, locker, pid, [123]),
    {error, not_found} = rpc:call(B, locker, pid, [123]),
    {error, not_found} = rpc:call(C, locker, pid, [123]),

    ok = rpc:call(A, locker, lock, [123, Pid]),
    {ok, Pid} = rpc:call(A, locker, pid, [123]),
    {ok, Pid} = rpc:call(B, locker, pid, [123]),
    {ok, Pid} = rpc:call(C, locker, pid, [123]),


    ok = rpc:call(B, locker, extend_lease, [123, Pid]),
    rpc:sbcast([A, B, C], locker, expire_leases),
    {ok, Pid} = rpc:call(A, locker, pid, [123]),
    {ok, Pid} = rpc:call(B, locker, pid, [123]),
    {ok, Pid} = rpc:call(C, locker, pid, [123]),

    ok.


%% qc(_) ->
%%     ?line true = eqc:quickcheck(delay_prop()).


%% delay_prop() ->
%%     ?FORALL(D, choose(0, 25),
%%             begin
%%                 [A, B, C] = setup(),
%%                 {ok, _Session1} = rpc:call(A, session, start_link, [123, 2]),

%%                 timer:sleep(D),
%%                 Res = rpc:call(A, gossiper, pid, [123]) =:= rpc:call(B, gossiper, pid, [123]),
%%                 teardown([A, B, C]),
%%                 Res
%%             end).



setup(NodeNames) ->
    Nodes = [element(2, slave:start(list_to_atom(net_adm:localhost()), N)) || N <- NodeNames],

    [rpc:call(N, code, add_path, ["/home/knutin/git/locker/ebin"]) || N <- Nodes],    [rpc:call(N, locker, start_link, [2]) || N <- Nodes],

    [begin
         {ok, _, _, R1, R2} = rpc:call(N, locker, get_debug_state, []),
         [{ok, cancel} = rpc:call(N, timer, cancel, [R]) || R <- [R1, R2]]
     end || N <- Nodes],

    Nodes.


teardown(Nodes) ->
    lists:map(fun slave:stop/1, Nodes).
