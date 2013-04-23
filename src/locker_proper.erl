-module(locker_proper).
-compile([export_all]).

-include_lib("proper/include/proper.hrl").

-record(state, {master_leases, replicated_leases}).

-define(MASTERS, [host_name("a")]).
-define(REPLICAS, [host_name("b")]).

test() ->
    proper:quickcheck(prop_lock_release()).

prop_lock_release() ->
    ?FORALL(Commands, parallel_commands(?MODULE),
            ?TRAPEXIT(
               begin
                   [A, B] = Cluster = setup([a, b]),
                   ok = rpc:call(A, locker, set_nodes, [Cluster, [A], [B]]),
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

get_master() ->
    elements(?MASTERS).

get_replica() ->
    elements(?REPLICAS).

get_node() ->
    elements(?MASTERS ++ ?REPLICAS).

is_master(N) ->
    lists:member(N, ?MASTERS).

is_replica(N) ->
    lists:member(N, ?REPLICAS).

command(S) ->
    Leases = S#state.master_leases =/= [],
    oneof([{call, ?MODULE, lock, [get_node(), key(), value()]}] ++
              [{call, ?MODULE, read, [get_node(), key()]}] ++
              [?LET({Key, Value}, elements(S#state.master_leases),
                    {call, ?MODULE, release,
                     [get_node(), Key, Value]}) || Leases] ++
              [{call, ?MODULE, update, [get_node(), key(), value(), value()]}
                || Leases] ++
              [{call, ?MODULE, replicate, []}]
         ).

lock(Node, Key, Value) ->
    rpc:call(Node, locker, lock, [Key, Value]).

release(Node, Key, Value) ->
    rpc:call(Node, locker, release, [Key, Value]).

update(Node, Key, Value, NewValue) ->
    rpc:call(Node, locker, update, [Key, Value, NewValue]).

replicate() ->
    rpc:sbcast(?MASTERS, locker, push_trans_log),
    timer:sleep(200).

read(Node, Key) ->
    rpc:call(Node, locker, dirty_read, [Key]).


initial_state() ->
    #state{master_leases = [], replicated_leases = []}.

precondition(S, {call, _, release, [_, Key, _Value]}) ->
    lists:keymember(Key, 1, S#state.master_leases);

precondition(_, _) ->
    true.

next_state(S, _V, {call, _, lock, [_, Key, Value]}) ->
    case lists:keymember(Key, 1, S#state.master_leases) of
        true ->
            S;
        false ->
            S#state{master_leases = [{Key, Value} | S#state.master_leases]}
    end;

next_state(S, _V, {call, _, release, [_, Key, Value]}) ->
    case lists:member({Key, Value}, S#state.master_leases) of
        true ->
            S#state{master_leases = lists:delete({Key, Value},
                                                 S#state.master_leases),
                    replicated_leases =
                        lists:delete({Key, Value}, S#state.replicated_leases)};
        false ->
            S
    end;

next_state(S, _V, {call, _, update, [_, Key, Value, NewValue]}) ->
    case lists:member({Key, Value}, S#state.master_leases) of
        true ->
            S#state{master_leases = [{Key, NewValue} |
                                        lists:delete({Key, Value},
                                                     S#state.master_leases)]};
        false ->
            S
    end;

next_state(S, _V, {call, _, replicate, []}) ->
    S#state{replicated_leases = S#state.master_leases};

next_state(S, _V, {call, _, read, _}) ->
    S.

postcondition(S, {call, _, lock, [_, Key, _Value]}, Result) ->
    case Result of
        {ok, _, _, _} ->
            not lists:keymember(Key, 1, S#state.master_leases);
        {error, no_quorum} ->
            lists:keymember(Key, 1, S#state.master_leases)
    end;

postcondition(S, {call, _, release, [_, Key, Value]}, {ok, _, _, _}) ->
    lists:member({Key, Value}, S#state.master_leases);

postcondition(S, {call, _, release, [_, Key, _Value]}, {error, no_quorum}) ->
    lists:keymember(Key, 1, S#state.master_leases);

postcondition(S, {call, _, update, [_, Key, Value, _NewValue]},
              {ok, _, _, _}) ->
    lists:member({Key, Value}, S#state.master_leases);

postcondition(S, {call, _, update, [_, Key, Value, _NewValue]},
              {error, no_quorum}) ->
    Val = lists:keymember(Key, 1, S#state.master_leases),
    Val orelse (Val =/= Value);

postcondition(_S, {call, _, replicate, []}, _) ->
    true;

postcondition(S, {call, _, read, [Node, Key]}, Result) ->
    case is_master(Node) of
        true ->
            case Result of
                {ok, Value} ->
                    lists:member({Key, Value}, S#state.master_leases);
                {error, not_found} ->
                    not lists:keymember(Key, 1, S#state.master_leases)
            end;
        false ->
            case Result of
                {ok, Value} ->
                    lists:member({Key, Value}, S#state.replicated_leases);
                {error, not_found} ->
                    not lists:keymember(Key, 1, S#state.replicated_leases)
            end
    end.

%%
%% SETUP
%%

setup(Name) when is_atom(Name) ->
    {ok, Node} = slave:start_link(list_to_atom(net_adm:localhost()), Name),
    true = rpc:call(Node, code, add_path, ["ebin"]),
    {ok, _} = rpc:call(Node, locker, start_link, [1]),

    {ok, _, _, R1, R2, R3} = rpc:call(Node, locker, get_debug_state, []),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R1]),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R2]),
    {ok, cancel} = rpc:call(Node, timer, cancel, [R3]),
    Node;

setup(NodeNames) ->
    lists:map(fun setup/1, NodeNames).

teardown(Nodes) ->
    lists:map(fun slave:stop/1, Nodes).

%% @doc Return fully qualified name for local host node.
host_name(Name) ->
    list_to_atom(Name ++ "@" ++ net_adm:localhost()).
