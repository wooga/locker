-module(basho_bench_driver_locker).

-export([new/1,
         run/4]).

new(_Id) ->
    case basho_bench_config:get(setup_completed) of
        true ->
            {ok, []};
        false ->
            net_kernel:start([a, longnames]),
            {ok, _LocalLocker} = locker:start_link(2),
            [B, C, D, E] = setup([b, c, d, e]),
            ok = locker:set_nodes([node(), B, C, D, E], [node(), B, C], [D, E]),
            basho_bench_config:set(setup_completed, true),
            {ok, []}
    end.

setup(NodeNames) ->
    Nodes = [element(2, slave:start(list_to_atom(net_adm:localhost()), N))
             || N <- NodeNames],

    [rpc:call(N, code, add_path, ["/home/knutin/git/locker/ebin"]) || N <- Nodes],
    [rpc:call(N, locker, start_link, [2]) || N <- Nodes],

    %% [begin
    %%      {ok, _, _, Ref} = rpc:call(N, locker, get_debug_state, []),
    %%      error_logger:info_msg("ref: ~p~n", [Ref]),
    %%      {ok, cancel} = rpc:call(N, timer, cancel, [Ref])
    %%  end || N <- Nodes],

    Nodes.


run(set, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case locker:lock(Key, self()) of
        {ok, _, _, _} ->
            {ok, State};
        {error, Error} ->
            error_logger:info_msg("Key: ~p~n", [Key]),
            {error, Error, State}
    end;

run(get, KeyGen, _, State) ->
    Key = KeyGen(),
    case locker:dirty_read(Key) of
        {ok, Pid} when Pid =:= self() ->
            {ok, State};
        {ok, _OtherPid} ->
            {error, wrong_pid_in_read, State};
        {error, not_found} ->
            {ok, State}
    end.

