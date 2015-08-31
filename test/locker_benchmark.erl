-module(locker_benchmark).
-compile([export_all]).



start() ->
    StatsConfig = [{docroot, filename:absname(
                               filename:join(code:priv_dir(statman),
                                             "docroot"))}],
    elli:start_link([{callback, statman_elli}, {callback_args, StatsConfig}]),

    locker:start_link(1),
    locker:set_nodes([node()], [node()], []),

    statman_server:start_link(1000),
    statman_merger:start_link(),

    statman_elli_server:start_link(),
    statman_server:add_subscriber(statman_merger),
    statman_merger:add_subscriber(statman_elli_server).


run(Start, End, ExtendInterval, LeaseLength) ->
    [begin
         timer:sleep(100),
         spawn(?MODULE, session, [N, ExtendInterval, LeaseLength])
     end || N <- lists:seq(Start, End)].



session(Id, ExtendInterval, LeaseLength) ->
    Start = erlang:monotonic_time(micro_seconds),
    case locker:lock(Id, self(), LeaseLength) of
        {ok, _, _, _} ->
            statman_histogram:record_value(get_lease, (erlang:monotonic_time(micro_seconds) - Start)),
            ?MODULE:session_loop(Id, ExtendInterval, LeaseLength);
        {error, _} ->
            error_logger:info_msg("~p: could not get lock~n", [Id])
    end.

session_loop(Id, ExtendInterval, LeaseLength) ->
    erlang:send_after(ExtendInterval, self(), extend),

    receive
        extend ->
            Start = erlang:monotonic_time(micro_seconds),
            case locker:extend_lease(Id, self(), LeaseLength) of
                ok ->
                    statman_histogram:record_value(extend_lease, (erlang:monotonic_time(micro_seconds) - Start)),
                    ?MODULE:session_loop(Id, ExtendInterval, LeaseLength);
                {error, _} ->
                    error_logger:info_msg("~p: could not extend lease~n", [Id])
            end
    end.
