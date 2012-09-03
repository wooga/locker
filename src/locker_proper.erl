-module(locker_proper).
-compile([export_all]).

-include_lib("proper/include/proper.hrl").

-record(state, {leases}).

test() ->
    proper:quickcheck(prop_lock_release()).

prop_lock_release() ->
    ?FORALL(Commands, parallel_commands(?MODULE),
            ?TRAPEXIT(
               begin
                   [A, _B, _C] = Cluster = setup([a, b, c]),
                   ok = rpc:call(A, locker, set_nodes, [Cluster, Cluster, []]),
                   {Seq, P, Result} = run_parallel_commands(?MODULE, Commands),
                   teardown(Cluster),
                   ?WHENFAIL(
                      io:format("Sequential: ~p\nParallel: ~p\nRes: ~p\n",
                                [Seq, P, Result]),
                      Result =:= ok)
               end)).


key() ->
    elements([1]).

value() ->
    elements([foo, bar]).

get_node() ->
    elements(['a@knutin', 'b@knutin']).

command(S) ->
    Leases = S#state.leases =/= [],
    oneof([{call, ?MODULE, lock, [get_node(), key(), value()]}] ++
              [?LET({Key, Value}, elements(S#state.leases),
                    {call, ?MODULE, release, [get_node(), Key, Value]}) || Leases]).


lock(Node, Key, Value) ->
    rpc:call(Node, locker, lock, [Key, Value]).

release(Node, Key, Value) ->
    rpc:call(Node, locker, release, [Key, Value]).


initial_state() ->
    #state{leases = []}.

precondition(S, {call, _, release, [_, Key, _Value]}) ->
    lists:keymember(Key, 1, S#state.leases);

precondition(_, _) ->
    true.

next_state(S, _V, {call, _, lock, [_, Key, Value]}) ->
    case lists:keymember(Key, 1, S#state.leases) of
        true ->
            S;
        false ->
            S#state{leases = [{Key, Value} | S#state.leases]}
    end;

next_state(S, _V, {call, _, release, [_, Key, Value]}) ->
    case lists:member({Key, Value}, S#state.leases) of
        true ->
            S#state{leases = lists:delete({Key, Value}, S#state.leases)};
        false ->
            S
    end.

postcondition(S, {call, _, lock, [_, Key, _Value]}, Result) ->
    case Result of
        {ok, _, _, _} ->
            not lists:keymember(Key, 1, S#state.leases);
        {error, no_quorum} ->
            lists:keymember(Key, 1, S#state.leases)
    end;


postcondition(S, {call, _, release, [_, Key, Value]}, {ok, _, _, _}) ->
    lists:member({Key, Value}, S#state.leases);

postcondition(S, {call, _, release, [_, Key, _Value]}, {error, no_quorum}) ->
    lists:keymember(Key, 1, S#state.leases).



%%
%% SETUP
%%

setup(Name) when is_atom(Name) ->
    {ok, Node} = slave:start_link(list_to_atom(net_adm:localhost()), Name),
    true = rpc:call(Node, code, add_path, ["ebin"]),
    {ok, _} = rpc:call(Node, locker, start_link, [2]),

    {ok, _, _, R1, R2, R3} = rpc:call(Node, locker, get_debug_state, []),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R1]),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R2]),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R3]),
    Node;

setup(NodeNames) ->
    lists:map(fun setup/1, NodeNames).


teardown(Nodes) ->
    lists:map(fun slave:stop/1, Nodes).
