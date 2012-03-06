-module(session).
-behaviour(gen_fsm).

%% API
-export([start_link/2, lock/1]).

%% gen_server callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-export([running/2, acquire_lock/2]).


-record(state, {key, init_tag, extend_lease_ref, commit_sleep}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Key, CommitSleep) ->
    gen_fsm:start_link(?MODULE, [Key, CommitSleep], []).

lock(Pid) ->
    gen_fsm:send_all_state_event(Pid, acquire_lock).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Key, CommitSleep]) ->
    {ok, acquire_lock, #state{key = Key, commit_sleep = CommitSleep}, 0}.

running(extend_lease, State) ->
    error_logger:info_msg("Extending lease~n"),
    case gossiper:extend_lease(State#state.key, self()) of
        ok ->
            {next_state, running, State};
        {error, not_owner} ->
            {stop, not_owner, State}
    end.


acquire_lock(timeout, State) ->
    error_logger:info_msg("acquiring lock~n"),
    case locker:nodes() of
        {ok, Nodes, W} ->
            error_logger:info_msg("Nodes: ~p, W: ~p~n", [Nodes, W]),

            {Tag, RequestReplies, _BadNodes} = locker:request_lock(Nodes, State#state.key,
                                                                   self()),
            timer:sleep(State#state.commit_sleep),
            error_logger:info_msg("request replies: ~p~n", [RequestReplies]),

            case ok_responses(RequestReplies) of
                OkNodes when length(OkNodes) >= W ->
                    %% Commit on all nodes
                    {CommitReplies, _} = locker:commit_lock(Nodes, Tag, State#state.key, self()),
                    error_logger:info_msg("commit replies: ~p~n", [CommitReplies]),
                    {next_state, running, State};
                _ ->
                    {AbortReplies, _} = locker:abort_lock(Nodes, Tag),
                    error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
                    {stop, no_majority, State}
            end;

            %% OtherPids = [P || {_, {error, already_registered, P}} <- Replies],

            %% case resolve_conflicts(self(), OtherPids) of
            %%     won ->
            %%         locker:won_election(Nodes, State#state.key, self()),
            %%         ExtendLeaseRef = schedule_lease_extend(),
            %%         {next_state, running, State#state{extend_lease_ref = ExtendLeaseRef}};
            %%     no_conflict ->
            %%         {next_state, running, State};
            %%     lost ->
            %%         {stop, already_locked, State}

        {error, minority} ->
            {stop, minority, State}
    end.

ok_responses(Replies) ->
    [R || {_, ok} = R <- Replies].




schedule_lease_extend() ->
    gen_fsm:send_event_after(1000, extend_lease).


handle_event(acquire_lock, _StateName, State) ->
    {next_state, acquire_lock, State, 0}.


handle_sync_event(acquire_lock, _StateName, _From, State) ->
    error_logger:info_msg("~p locking at ~p~n", [self(), State#state.key]),
    case gossiper:lock(State#state.key, self()) of
        ok ->
            {reply, ok, State};
        Error ->
            {reply, Error, State}
    end.



handle_info(Msg, StateName, State) ->
    error_logger:info_msg("got unexpected msg: ~p~n", [Msg]),
    {next_state, StateName, State}.


terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

wait_for_replies(0, _, _) ->
    [];
wait_for_replies(N, Tag, Type) ->
    receive
        {locker, Tag, Node, Type, Msg} ->
            [{Node, Msg} | wait_for_replies(N-1, Tag, Type)]
    after 1000 ->
            []
    end.

resolve_conflicts(_, []) ->
    no_conflict;
resolve_conflicts(Self, [Opponent | Rest]) ->
    case Self > Opponent of
        true ->
            case Rest of
                [] ->
                    won;
                _ ->
                    resolve_conflicts(Self, Rest)
            end;
        false ->
            lost
    end.
