-module(basho_bench_driver_locker).

-export([new/1,
         run/4]).

new(_Id) ->
    case mark_setup_completed() of
        true ->
            error_logger:info_msg("setting up cluster~n"),
            net_kernel:start([master, shortnames]),
            {ok, _LocalLocker} = locker:start_link(2),
            MasterNames = basho_bench_config:get(masters),
            ReplicaNames = basho_bench_config:get(replicas),
            W = basho_bench_config:get(w),

            case basho_bench_config:get(start_nodes) of
                true ->
                    Masters = setup(MasterNames, W),
                    Replicas = setup(ReplicaNames, W),

                    ok = locker:set_nodes(Masters ++ Replicas, Masters, Replicas),
                    error_logger:info_msg("~p~n",
                                          [rpc:call(hd(Replicas), locker, get_meta, [])]),
                    {ok, {Masters, Replicas}};
                false ->
                    {ok, []}
            end;
        false ->
            %%timer:sleep(30000),
            Masters = [list_to_atom(atom_to_list(N) ++ "@" ++ atom_to_list(H))
                       || {H, N} <- basho_bench_config:get(masters)],

            Replicas = [list_to_atom(atom_to_list(N) ++ "@" ++ atom_to_list(H))
                        || {H, N} <- basho_bench_config:get(replicas)],

            {ok, {Masters, Replicas}}
    end.

setup(NodeNames, W) ->
    Nodes = [begin
                 element(2, slave:start_link(Hostname, N))
             end
             || {Hostname, N} <- NodeNames],

    [rpc:call(N, code, add_path, ["/home/knutin/git/locker/ebin"]) || N <- Nodes],
    [rpc:call(N, locker, start_link, [W]) || N <- Nodes],

    Nodes.


mark_setup_completed() ->
    case whereis(locker_setup) of
        undefined ->
            true = register(locker_setup, self()),
            true;
        _ ->
            false
    end.



run(set, KeyGen, _ValueGen, {[M | Masters], Replicas}) ->
    NewMasters = lists:reverse([M | lists:reverse(Masters)]),

    Key = KeyGen(),
    case rpc:call(M, locker, lock, [Key, Key]) of
        {ok, _, _, _} ->
            {ok, {NewMasters, Replicas}};
        {error, Error} ->
            error_logger:info_msg("Key: ~p~, ~p~n", [Key, Error]),
            {error, Error, {NewMasters, Replicas}}
    end;

run(get, KeyGen, _, {[M | Masters], Replicas}) ->
    NewMasters = lists:reverse([M | lists:reverse(Masters)]),

    Key = KeyGen(),
    case locker:dirty_read(Key) of
        {ok, Key} ->
            {ok, {NewMasters, Replicas}};
        {ok, _OtherValue} ->
            {error, wrong_value, {NewMasters, Replicas}};
        {error, not_found} ->
            {ok, {NewMasters, Replicas}}
    end.

