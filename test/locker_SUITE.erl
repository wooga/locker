-module(locker_SUITE).
-compile([export_all]).
-include_lib("test_server/include/test_server.hrl").
-include_lib("eqc/include/eqc.hrl").

all() ->
    [
     quorum,
     no_quorum_possible,
     node_liveness
    ].
%%     qc].

quorum(_) ->
    [A, B, C] = setup(),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {1, catch rpc:call(A, locker, lock, [123, self()])}
          end),
    spawn(fun() ->
                  Parent ! {2, catch rpc:call(B, locker, lock, [123, self()])}
          end),
    receive {1, P1} -> P1 after 1000 -> throw(timeout) end,
    receive {2, P2} -> P2 after 1000 -> throw(timeout) end,

    ?line {ok, Pid} = rpc:call(A, locker, pid, [123]),
    ?line {ok, Pid} = rpc:call(B, locker, pid, [123]),
    ?line {ok, Pid} = rpc:call(C, locker, pid, [123]),

    {ok, [], [{123, {Pid, _}}]} = rpc:call(A, locker, get_debug_state, []),
    {ok, [], [{123, {Pid, _}}]} = rpc:call(B, locker, get_debug_state, []),
    {ok, [], [{123, {Pid, _}}]} = rpc:call(C, locker, get_debug_state, []),

    teardown([A, B, C]).

no_quorum_possible(_) ->
    [A, B, C] = setup(),

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

    {ok, [], []} = rpc:call(A, locker, get_debug_state, []),
    {ok, [], []} = rpc:call(B, locker, get_debug_state, []),
    {ok, [], []} = rpc:call(C, locker, get_debug_state, []),

    teardown([A, B, C]).


node_liveness(_) ->
    [A, B, C] = setup(),
    timer:sleep(5000),

    teardown([A, B, C]).

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



setup() ->
    {ok, A} = slave:start(list_to_atom(net_adm:localhost()), a),
    {ok, B} = slave:start(list_to_atom(net_adm:localhost()), b),
    {ok, C} = slave:start(list_to_atom(net_adm:localhost()), c),
    true = rpc:call(A, code, add_path, ["/home/knutin/git/locker/ebin"]),
    true = rpc:call(B, code, add_path, ["/home/knutin/git/locker/ebin"]),
    true = rpc:call(C, code, add_path, ["/home/knutin/git/locker/ebin"]),

    rpc:call(A, locker, start_link, [3, 2]),
    rpc:call(B, locker, start_link, [3, 2]),
    rpc:call(C, locker, start_link, [3, 2]),


    rpc:call(A, locker, add_node, [B]),
    rpc:call(A, locker, add_node, [C]),
    rpc:call(B, locker, add_node, [A]),
    rpc:call(B, locker, add_node, [C]),
    rpc:call(C, locker, add_node, [A]),
    rpc:call(C, locker, add_node, [B]),

    [A, B, C].


teardown(Nodes) ->
    lists:map(fun slave:stop/1, Nodes).
