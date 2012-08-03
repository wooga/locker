%% @doc Distributed consistent key-value store
%%
%% Reads use the local copy, all data is replicated to all nodes.
%%
%% Writing is done in two phases, in the first phase the key is
%% locked, if a quorum can be made, the value is written.

-module(locker).
-behaviour(gen_server).
-author('Knut Nesheim <knutin@gmail.com>').

%% API
-export([start_link/1, remove_node/1, set_w/2]).
-export([set_nodes/3]).

-export([lock/2, lock/3, extend_lease/3, release/2]).
-export([dirty_read/1]).
-export([lag/0, summary/0]).


-export([get_write_lock/4, do_write/6, release_write_lock/3]).
-export([get_nodes/0, get_debug_state/0]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          %% w is the number of nodes required to make the write
          %% quorum, this must be set manually for now as there is no
          %% liveness checking of nodes and the introduction of a new
          %% node is still very much a manual process
          w,

          %% Nodes participating in the quorums
          nodes = [],

          %% Replicas are receiving writes, but do not participate in
          %% the quorums
          replicas = [],

          %% A list of write-locks, only one lock per key is allowed,
          %% but a list is used for performance as there will be few
          %% concurrent locks
          locks = [], %% {tag, key, pid, now}


          %% The masters queue writes in the trans_log for batching to
          %% the replicas, triggered every N milliseconds by the
          %% push_replica timer
          trans_log = [],

          %% Timer references
          lease_expire_ref,
          write_locks_expire_ref,
          push_trans_log_ref
}).

-define(LEASE_LENGTH, 2000).
-define(DB, locker_db).

%%%===================================================================
%%% API
%%%===================================================================

start_link(W) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [W], []).

get_nodes() ->
    get_nodes(5000).

get_nodes(Timeout) ->
    gen_server:call(?MODULE, get_nodes, Timeout).

lock(Key, Value) ->
    lock(Key, Value, ?LEASE_LENGTH).

lock(Key, Value, LeaseLength) ->
    lock(Key, Value, LeaseLength, 5000).

%% @doc: Tries to acquire the lock. In case of unreachable nodes, the
%% timeout is 1 second per node which might need tuning. Returns {ok,
%% W, V, C} where W is the number of agreeing nodes required for a
%% quorum, V is the number of nodes that voted in favor of this lock
%% in the case of contention and C is the number of nodes who
%% acknowledged commit of the lock successfully.
lock(Key, Value, LeaseLength, Timeout) ->
    {ok, Nodes, _Replicas, W} = get_nodes(Timeout),

    %% Try getting the write lock on all nodes
    {Tag, RequestReplies, _BadNodes} = get_write_lock(Nodes, Key, not_found, Timeout),

    case ok_responses(RequestReplies) of
        {OkNodes, _} when length(OkNodes) >= W ->
            %% Majority of nodes gave us the lock, go ahead and do the
            %% write on all nodes. The write also releases the lock

            {WriteReplies, _} = do_write(Nodes,
                                         Tag, Key, Value,
                                         LeaseLength, Timeout),
            {OkWrites, _} = ok_responses(WriteReplies),
            {ok, W, length(OkNodes), length(OkWrites)};
        _ ->
            {_AbortReplies, _} = release_write_lock(Nodes, Tag, Timeout),
            {error, no_quorum}
    end.

release(Key, Value) ->
    release(Key, Value, 5000).

release(Key, Value, Timeout) ->
    {ok, Nodes, Replicas, W} = get_nodes(Timeout),

    %% Try getting the write lock on all nodes
    {Tag, WriteLockReplies, _} = get_write_lock(Nodes, Key, Value, Timeout),

    case ok_responses(WriteLockReplies) of
        {OkNodes, _} when length(OkNodes) >= W ->
            Request = {release, Key, Value, Tag},
            {ReleaseReplies, _BadNodes} =
                gen_server:multi_call(Nodes ++ Replicas, locker, Request, Timeout),

            {OkWrites, _} = ok_responses(ReleaseReplies),

            {ok, W, length(OkNodes), length(OkWrites)};
        _ ->
            {_AbortReplies, _} = release_write_lock(Nodes, Tag, Timeout),
            {error, no_quorum}
    end.


extend_lease(Key, Value, LeaseLength) ->
    extend_lease(Key, Value, LeaseLength, 5000).

%% @doc: Extends the lease for the lock on all nodes that are up. What
%% really happens is that the expiration is scheduled for (now + lease
%% time), to allow for nodes that just joined to set the correct
%% expiration time without knowing the start time of the lease.
extend_lease(Key, Value, LeaseLength, Timeout) ->
    {ok, Nodes, _Replicas, W} = get_nodes(Timeout),
    {Tag, WriteLockReplies, _} = get_write_lock(Nodes, Key, Value, Timeout),

    case ok_responses(WriteLockReplies) of
        {N, _E} when length(N) >= W ->

            Request = {extend_lease, Tag, Key, Value, LeaseLength},
            {Replies, _} =
                gen_server:multi_call(Nodes, locker, Request, Timeout),
            {_, FailedExtended} = ok_responses(Replies),
            release_write_lock(FailedExtended, Tag, Timeout),
            ok;
        _ ->
            {_AbortReplies, _} = release_write_lock(Nodes, Tag, Timeout),
            {error, no_quorum}
    end.

%% @doc: A dirty read does not create a read-quorum so consistency is
%% not guaranteed. The value is read directly from a local ETS-table,
%% so the performance should be very high.
dirty_read(Key) ->
    case ets:lookup(?DB, Key) of
        [{Key, Value, _Lease}] ->
            {ok, Value};
        [] ->
            {error, not_found}
    end.

%%
%% Operations
%%

lag() ->
    {Time, Result} = timer:tc(fun() -> lock('__lock_lag_probe', foo, 10) end),
    {Time / 1000, Result}.

summary() ->
    {ok, WriteLocks, Leases, _LeaseExpireRef, _WriteLocksExpireRef} = get_debug_state(),
    [{write_locks, length(WriteLocks)},
     {leases, length(Leases)}].



%%
%% Helpers
%%

get_write_lock(Nodes, Key, Value, Timeout) ->
    Tag = make_ref(),
    Request = {get_write_lock, Key, Value, Tag},
    {Replies, Down} = gen_server:multi_call(Nodes, locker, Request, Timeout),
    {Tag, Replies, Down}.

do_write(Nodes, Tag, Key, Value, LeaseLength, Timeout) ->
    gen_server:multi_call(Nodes, locker, {write, Tag, Key, Value, LeaseLength}, Timeout).


release_write_lock(Nodes, Tag, Timeout) ->
    gen_server:multi_call(Nodes, locker, {release_write_lock, Tag}, Timeout).



%% @doc: Replaces the primary and replica node list on all nodes in
%% the cluster. Assumes no failures.
set_nodes(Cluster, Primaries, Replicas) ->
    {_Replies, []} = gen_server:multi_call(Cluster, locker,
                                           {set_nodes, Primaries, Replicas}),
    ok.

set_w(Cluster, W) when is_integer(W) ->
    {_Replies, []} = gen_server:multi_call(Cluster, locker, {set_w, W}),
    ok.

remove_node(Node) ->
    gen_server:call(?MODULE, {remove_node, Node, true}).

get_debug_state() ->
    gen_server:call(?MODULE, get_debug_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([W]) ->
    ?DB = ets:new(?DB, [named_table, protected,
                        {read_concurrency, true},
                        {write_concurrency, true}]),
    {ok, LeaseExpireRef} = timer:send_interval(10000, expire_leases),
    {ok, WriteLocksExpireRef} = timer:send_interval(1000, expire_locks),
    {ok, PushTransLog} = timer:send_interval(100, push_trans_log),
    {ok, #state{w = W,
                nodes = ordsets:new(),
                lease_expire_ref = LeaseExpireRef,
                write_locks_expire_ref = WriteLocksExpireRef,
                push_trans_log_ref = PushTransLog}}.


%%
%% WRITE-LOCKS
%%

handle_call({get_write_lock, Key, Value, Tag}, _From,
            #state{locks = Locks} = State) ->
    %% Phase 1: Grant a write lock on the key if the value in the
    %% database is what the coordinator expects. If the atom
    %% 'not_found' is given as the expected value, the lock is granted
    %% if the key does not exist.
    %%
    %% Only one lock per key is allowed. Timeouts are triggered when
    %% handling 'expire_locks'

    case is_locked(Key, Locks) of
        true ->
            {reply, {error, already_locked}, State};
        false ->
            %% Only grant the lock if the current value is what the
            %% coordinator expects
            case ets:lookup(?DB, Key) of
                [{Key, DbValue, _Expire}] when DbValue =:= Value ->
                    NewLocks = [{Tag, Key, Value, now_to_ms()} | Locks],
                    {reply, ok, State#state{locks = NewLocks}};
                [] when Value =:= not_found->
                    NewLocks = [{Tag, Key, Value, now_to_ms()} | Locks],
                    {reply, ok, State#state{locks = NewLocks}};
                _Other ->
                    {reply, {error, not_expected_value}, State}
            end
    end;

handle_call({release_write_lock, Tag}, _From, #state{locks = Locks} = State) ->
    case lists:keytake(Tag, 1, Locks) of
        {value, {Tag, _Key, _, _}, NewLocks} ->
            {reply, ok, State#state{locks = NewLocks}};
        false ->
            {reply, {error, lock_expired}, State}
    end;


%%
%% DATABASE OPERATIONS
%%

handle_call({write, LockTag, Key, Value, LeaseLength}, _From,
            #state{locks = Locks, trans_log = TransLog} = State) ->
    %% Database write. LockTag might be a valid write-lock, in which
    %% case it is deleted to avoid the extra round-trip of explicit
    %% delete. If it is not valid, we assume the coordinator had a
    %% quorum before writing.

    NewLocks = lists:keydelete(LockTag, 1, Locks),
    NewTransLog = [{write, Key, Value, LeaseLength} | TransLog],
    true = ets:insert(?DB, {Key, Value, now_to_ms() + LeaseLength}),
    {reply, ok, State#state{locks = NewLocks, trans_log = NewTransLog}};


%%
%% LEASES
%%

handle_call({extend_lease, LockTag, Key, Value, ExtendLength}, _From,
            #state{locks = Locks, trans_log = TransLog} = State) ->
    %% Extending a lease sets a new expire time. As the coordinator
    %% holds a write lock on the key, it is not necessary to perform
    %% any validation.

    case ets:lookup(?DB, Key) of
        [{Key, Value, _}] ->
            NewLocks = lists:keydelete(LockTag, 1, Locks),
            NewTransLog = [{write, Key, Value, ExtendLength} | TransLog],
            true = ets:insert(?DB, {Key, Value, now_to_ms() + ExtendLength}),
            {reply, ok, State#state{locks = NewLocks, trans_log = NewTransLog}};

        [{Key, _OtherValue, _}] ->
            {reply, {error, not_owner}, State};
        [] ->
            %% Not found, so create it if we are a replica
            case ordsets:is_element(node(), State#state.replicas) of
                true ->
                    true = ets:insert(?DB, {Key, Value, now_to_ms() + ExtendLength}),
                    {reply, ok, State};
                false ->
                    {reply, {error, not_found}, State}
            end
    end;


handle_call({release, Key, Value, LockTag}, _From,
            #state{locks = Locks, trans_log = TransLog} = State) ->
    case ets:lookup(?DB, Key) of
        [{Key, Value, _Lease}] ->
            NewLocks = lists:keydelete(LockTag, 1, Locks),
            NewTransLog = [{delete, Key} | TransLog],
            true = ets:delete(?DB, Key),
            {reply, ok, State#state{locks = NewLocks, trans_log = NewTransLog}};

        [{Key, _OtherPid, _}] ->
            {reply, {error, not_owner}, State};
        [] ->
            {reply, {error, not_found}, State}
    end;




%%
%% ADMINISTRATION
%%

handle_call(get_nodes, _From, State) ->
    {reply, {ok,
             State#state.nodes,
             State#state.replicas,
             State#state.w}, State};

handle_call({set_w, W}, _From, State) ->
    {reply, ok, State#state{w = W}};

handle_call({set_nodes, Primaries, Replicas}, _From, State) ->
    {reply, ok, State#state{nodes = ordsets:from_list(Primaries),
                            replicas = ordsets:from_list(Replicas)}};


handle_call({remove_node, Node, Reverse}, _From,
            #state{nodes = Nodes} = State) ->
    NewNodes = ordsets:del_element(Node, Nodes),
    not Reverse orelse gen_server:call({locker, Node},
                                       {remove_node, node(), false}),
    {reply, ok, State#state{nodes = NewNodes}};


handle_call(get_debug_state, _From, State) ->
    {reply, {ok, State#state.locks,
             ets:tab2list(?DB),
             State#state.lease_expire_ref,
             State#state.write_locks_expire_ref}, State}.


%%
%% REPLICATION
%%

handle_cast({trans_log, _FromNode, TransLog}, State) ->
    %% Replay transaction log. Every master pushes it's log to us and
    %% for now we blindly write whatever we get. Hopefully we won't
    %% get interleaved write and deletes for the same key.

    %% In the future, we might want to offset the lease length in the
    %% master before writing it to the log to ensure the lease length
    %% is at least reasonably similar for all replicas.

    lists:foreach(fun ({write, Key, Value, LeaseLength}) ->
                          ets:insert(?DB, {Key, Value, now_to_ms() + LeaseLength});
                      ({delete, Key}) ->
                          ets:delete(?DB, Key)
                  end, TransLog),
    {noreply, State};


handle_cast({extend_lease, _LockTag, Key, Value, ExtendLength}, State) ->
    %% Replicated extend_lease

    case ordsets:is_element(node(), State#state.replicas) of
        true ->
            true = ets:insert(?DB, {Key, Value, now_to_ms() + ExtendLength});
        false ->
            noop
    end,
    {noreply, State};


handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(expire_leases, State) ->
    %% Run through each element in the ETS-table checking for expired
    %% keys so we can at the same time check if the key is locked. If
    %% we would use select_delet/2, we could not check for locks and
    %% we would still have to scan the entire table.
    %%
    %% If expiration of many keys becomes too expensive, we could keep
    %% a priority queue mapping expire to key.

    Now = now_to_ms(),
    ExpiredKeys = lists:foldl(
                    fun ({Key, _Value, ExpireTime}, Acc) ->
                            case is_expired(ExpireTime, Now)
                                andalso not is_locked(Key, State#state.locks) of
                                true ->
                                    [Key | Acc];
                                false ->
                                    Acc
                            end
                    end, [], ets:tab2list(?DB)),

    lists:foreach(fun (Key) -> ets:delete(?DB, Key) end, ExpiredKeys),
    {noreply, State};


handle_info(expire_locks, #state{locks = Locks} = State) ->
    Now = now_to_ms(),
    NewLocks = [L || {_, _, StartTimeMs} = L <- Locks, StartTimeMs + 1000 > Now],
    {noreply, State#state{locks = NewLocks}};

handle_info(push_trans_log, #state{trans_log = TransLog} = State) ->
    %% Push transaction log to *all* replicas. With multiple masters,
    %% each replica will receive the same write multiple times.

    gen_server:abcast(State#state.replicas, locker, {trans_log, node(), TransLog}),
    {noreply, State#state{trans_log = TransLog}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

now_to_ms() ->
    now_to_ms(now()).

now_to_ms({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + MicroSecs div 1000.

is_locked(Key, P) ->
    lists:keymember(Key, 2, P).

is_expired(ExpireTime, NowMs)->
    ExpireTime < NowMs.

ok_responses(Replies) ->
    lists:partition(fun ({_, ok}) -> true;
                        (_)        -> false
                    end, Replies).
