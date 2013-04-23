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
-export([start_link/1, start_link/4]).
-export([set_w/2, set_nodes/3]).

-export([lock/2, lock/3, lock/4, update/3, update/4,
         extend_lease/3,release/2, release/3]).
-export([wait_for/2, wait_for_release/2]).
-export([dirty_read/1, master_dirty_read/1]).
-export([lag/0, summary/0]).


-export([get_write_lock/4, do_write/6, release_write_lock/3]).
-export([get_meta/0, get_meta_ets/1, get_debug_state/0]).

-export([now_to_seconds/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          %% The masters queue writes in the trans_log for batching to
          %% the replicas, triggered every N milliseconds by the
          %% push_replica timer
          trans_log = [],

          %% Clients can wait for a key to become locked
          waiters = [],

          %% Clients can wait for a lock to be released
          release_waiters = [],

          %% Previous point of expiration, no keys older than this
          %% point should exist
          prev_expire_point,

          %% Timer references
          lease_expire_ref,
          write_locks_expire_ref,
          push_trans_log_ref
}).

-define(LEASE_LENGTH, 2000).
-define(DB, locker_db).
-define(LOCK_DB, locker_lock_db).
-define(META_DB, locker_meta_db).
-define(EXPIRE_DB, locker_expire_db).

%%%===================================================================
%%% API
%%%===================================================================

start_link(W) ->
    start_link(W, 1000, 1000, 100).

start_link(W, LeaseExpireInterval, LockExpireInterval, PushTransInterval) ->
    Args = [W, LeaseExpireInterval, LockExpireInterval, PushTransInterval],
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

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
    Nodes = get_meta_ets(nodes),
    W = get_meta_ets(w),

    %% Try getting the write lock on all nodes
    {Tag, RequestReplies, _BadNodes} = get_write_lock(Nodes, Key, not_found, Timeout),

    case ok_responses(RequestReplies) of
        {OkNodes, _} when length(OkNodes) >= W ->
            %% Majority of nodes gave us the lock, go ahead and do the
            %% write on all masters. The write also releases the
            %% lock. Replicas are synced asynchronously by the
            %% masters.
            {WriteReplies, _} = do_write(Nodes,
                                         Tag, Key, Value,
                                         LeaseLength, Timeout),
            {OkWrites, _} = ok_responses(WriteReplies),
            {ok, W, length(OkNodes), length(OkWrites)};
        _ ->
            {_AbortReplies, _} = release_write_lock(Nodes, Tag, Timeout),
            {error, no_quorum}
    end.

update(Key, Value, NewValue) ->
    update(Key, Value, NewValue, 5000).

%% @doc: Tries to update the lock. The update only happens if an existing
%% value of the lock corresponds to the given Value within the W number of
%% master nodes.
%% Returns the same tuple as in lock/4 case.
update(Key, Value, NewValue, Timeout) ->
    Nodes = get_meta_ets(nodes),
    W = get_meta_ets(w),

    %% Try getting the write lock on all nodes
    {Tag, RequestReplies, _BadNodes} = get_write_lock(Nodes, Key, Value,
                                                      Timeout),

    case ok_responses(RequestReplies) of
        {OkNodes, _} when length(OkNodes) >= W ->
            {UpdateReplies, _} = do_update(Nodes, Tag, Key, NewValue, Timeout),
            {OkUpdates, _} = ok_responses(UpdateReplies),
            {ok, W, length(OkNodes), length(OkUpdates)};
        _ ->
            {_AbortReplies, _} = release_write_lock(Nodes, Tag, Timeout),
            {error, no_quorum}
    end.

%% @doc: Waits for the key to become available on the local node. If a
%% value is already available, returns immediately, otherwise it will
%% return within the timeout. In case of timeout, the caller might get
%% a reply anyway if it sent at the same time as the timeout.
wait_for(Key, Timeout) ->
    case dirty_read(Key) of
        {ok, Value} ->
            {ok, Value};
        {error, not_found} ->
            gen_server:call(locker, {wait_for, Key, Timeout}, Timeout)
    end.

wait_for_release(Key, Timeout) ->
    case dirty_read(Key) of
        {ok, _Value} ->
            gen_server:call(locker, {wait_for_release, Key, Timeout}, Timeout);
        {error, not_found} ->
            {error, key_not_locked}
    end.

release(Key, Value) ->
    release(Key, Value, 5000).

release(Key, Value, Timeout) ->
    Nodes = get_meta_ets(nodes),
    Replicas = get_meta_ets(replicas),
    W = get_meta_ets(w),

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
    Nodes = get_meta_ets(nodes),
    W = get_meta_ets(w),

    {Tag, WriteLockReplies, _} = get_write_lock(Nodes, Key, Value, Timeout),

    case ok_responses(WriteLockReplies) of
        {N, _E} when length(N) >= W ->

            Request = {extend_lease, Tag, Key, Value, LeaseLength},
            {Replies, _} = gen_server:multi_call(Nodes, locker, Request, Timeout),
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

%% @doc: Execute a dirty read on the master. Same caveats as for
%% dirty_read/1
master_dirty_read(Key) ->
    [Master | _Masters] = get_meta_ets(nodes),
    rpc:call(Master, locker, dirty_read, [Key]).

%%
%% Helpers for operators
%%

lag() ->
    Key = {'__lock_lag_probe', os:timestamp()},
    {Time, Result} = timer:tc(fun() ->
                                      lock(Key, foo, 2000)
                              end),
    release(Key, foo),
    {Time / 1000, Result}.

summary() ->
    {ok, WriteLocks, Leases,
     _LeaseExpireRef, _WriteLocksExpireRef, _PushTranslogRef} =
        get_debug_state(),
    [{write_locks, length(WriteLocks)},
     {leases, length(Leases)}].

get_meta() ->
    {get_meta_ets(nodes), get_meta_ets(replicas), get_meta_ets(w)}.

%%
%% Helpers
%%

get_write_lock(Nodes, Key, Value, Timeout) ->
    Tag = make_ref(),
    Request = {get_write_lock, Key, Value, Tag},
    {Replies, Down} = gen_server:multi_call(Nodes, locker, Request, Timeout),
    {Tag, Replies, Down}.

do_write(Nodes, Tag, Key, Value, LeaseLength, Timeout) ->
    gen_server:multi_call(Nodes, locker,
                          {write, Tag, Key, Value, LeaseLength},
                          Timeout).

do_update(Nodes, Tag, Key, Value, Timeout) ->
    gen_server:multi_call(Nodes, locker,
                          {update, Tag, Key, Value},
                          Timeout).

release_write_lock(Nodes, Tag, Timeout) ->
    gen_server:multi_call(Nodes, locker, {release_write_lock, Tag}, Timeout).

get_meta_ets(Key) ->
    case ets:lookup(?META_DB, Key) of
        [] ->
            throw({locker, no_such_meta_key});
        [{Key, Value}] ->
            Value
    end.

%% @doc: Replaces the primary and replica node list on all nodes in
%% the cluster. Assumes no failures.
set_nodes(Cluster, Primaries, Replicas) ->
    {_Replies, []} = gen_server:multi_call(Cluster, locker,
                                           {set_nodes, Primaries, Replicas}),
    ok.

set_w(Cluster, W) when is_integer(W) ->
    {_Replies, []} = gen_server:multi_call(Cluster, locker, {set_w, W}),
    ok.

get_debug_state() ->
    gen_server:call(?MODULE, get_debug_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([W, LeaseExpireInterval, LockExpireInterval, PushTransInterval]) ->
    ?DB = ets:new(?DB, [named_table, protected, set,
                        {read_concurrency, true},
                        {write_concurrency, true}]),

    ?LOCK_DB = ets:new(?LOCK_DB, [named_table, protected, set]),
    ?EXPIRE_DB = ets:new(?EXPIRE_DB, [named_table, protected, bag]),


    ?META_DB = ets:new(?META_DB, [named_table, protected, set,
                                  {read_concurrency, true}]),
    ets:insert(?META_DB, {w, W}),
    ets:insert(?META_DB, {nodes, []}),
    ets:insert(?META_DB, {replicas, []}),


    {ok, LeaseExpireRef} = timer:send_interval(LeaseExpireInterval, expire_leases),
    {ok, WriteLocksExpireRef} = timer:send_interval(LockExpireInterval, expire_locks),
    {ok, PushTransLog} = timer:send_interval(PushTransInterval, push_trans_log),
    {ok, #state{lease_expire_ref = LeaseExpireRef,
                write_locks_expire_ref = WriteLocksExpireRef,
                push_trans_log_ref = PushTransLog,
                prev_expire_point = now_to_seconds()}}.

%%
%% WRITE-LOCKS
%%

handle_call({get_write_lock, Key, Value, Tag}, _From, State) ->
    %% Phase 1: Grant a write lock on the key if the value in the
    %% database is what the coordinator expects. If the atom
    %% 'not_found' is given as the expected value, the lock is granted
    %% if the key does not exist.
    %%
    %% Only one lock per key is allowed. Timeouts are triggered when
    %% expiring leases.

    case is_locked(Key) of
        true ->
            %% Key already has a write lock
            {reply, {error, already_locked}, State};
        false ->
            case ets:lookup(?DB, Key) of
                [{Key, DbValue, _Expire}] when DbValue =:= Value ->
                    set_lock(Tag, Key),
                    {reply, ok, State};
                [] when Value =:= not_found->
                    set_lock(Tag, Key),
                    {reply, ok, State};
                _Other ->
                    {reply, {error, not_expected_value}, State}
            end
    end;

handle_call({release_write_lock, Tag}, _From, State) ->
    del_lock(Tag),
    {reply, ok, State};

%%
%% DATABASE OPERATIONS
%%

handle_call({write, LockTag, Key, Value, LeaseLength}, _From,
            #state{trans_log = TransLog} = State) ->
    %% Database write. LockTag might be a valid write-lock, in which
    %% case it is deleted to avoid the extra round-trip of explicit
    %% delete. If it is not valid, we assume the coordinator had a
    %% quorum before writing.
    del_lock(LockTag),
    ExpireAt = expire_at(LeaseLength),
    ets:insert(?DB, {Key, Value, ExpireAt}),
    schedule_expire(ExpireAt, Key),

    NewTransLog = [{write, Key, Value, LeaseLength} | TransLog],
    {reply, ok, State#state{trans_log = NewTransLog}};

handle_call({update, LockTag, Key, Value}, _From,
            #state{trans_log = TransLog} = State) ->
    del_lock(LockTag),

    case ets:lookup(?DB, Key) of
        [{Key, _Value, ExpireAt}] ->
            %% Update the lock
            ets:insert(?DB, {Key, Value, ExpireAt});
        [] ->
            %% Lock not found (most likely it has expired after acquiring write
            %% lock)
            ok
    end,

    NewTransLog = [{update, Key, Value} | TransLog],
    {reply, ok, State#state{trans_log = NewTransLog}};

%%
%% LEASES
%%

handle_call({extend_lease, LockTag, Key, Value, ExtendLength}, _From,
            #state{trans_log = TransLog} = State) ->
    %% Extending a lease sets a new expire time. As the coordinator
    %% holds a write lock on the key, it validation has already been
    %% done

    del_lock(LockTag),
    delete_expire(expires(Key), Key),

    ExpireAt = expire_at(ExtendLength),
    ets:insert(?DB, {Key, Value, ExpireAt}),
    schedule_expire(ExpireAt, Key),

    NewTransLog = [{extend_lease, Key, Value, ExtendLength} | TransLog],
    {reply, ok, State#state{trans_log = NewTransLog}};


handle_call({release, Key, Value, LockTag}, _From,
            #state{trans_log = TransLog} = State) ->
    {Reply, NewState} =
        case ets:lookup(?DB, Key) of
            [{Key, Value, ExpireAt}] ->
                del_lock(LockTag),
                ets:delete(?DB, Key),
                delete_expire(ExpireAt, Key),

                NewTransLog = [{release, Key} | TransLog],
                {ok, State#state{trans_log = NewTransLog}};

            [{Key, _OtherValue, _}] ->
                {{error, not_owner}, State};
            [] ->
                {{error, not_found}, State}
        end,
    NewWaiters = notify_release_waiter(Key, released, NewState#state.release_waiters),
    {reply, Reply, NewState#state{release_waiters = NewWaiters}};

%%
%% WAIT-FOR
%%

handle_call({wait_for, Key, Timeout}, From, #state{waiters = Waiters} = State) ->
    %% 'From' waits for the given key to become available, using
    %% gen_server:call/3. We will reply when replaying the transaction
    %% log. If we do not have a response within the given timeout, the
    %% reply is discarded.
    {noreply, State#state{waiters = [{Key, From, now_to_ms() + Timeout} | Waiters]}};

handle_call({wait_for_release, Key, Timeout}, From,
            #state{release_waiters = Waiters} = State) ->
    %% 'From' waits for the given key lock to become released, using
    %% gen_server:call/3. We will reply when replaying the transaction
    %% log. If we do not have a response within the given timeout, the
    %% reply is discarded.
    {noreply, State#state{release_waiters = [{Key, From, now_to_ms() + Timeout} | Waiters]}};

%%
%% ADMINISTRATION
%%

handle_call({set_w, W}, _From, State) ->
    ets:insert(?META_DB, {w, W}),
    {reply, ok, State};

handle_call({set_nodes, Primaries, Replicas}, _From, State) ->
    ets:insert(?META_DB, {nodes, ordsets:to_list(
                                   ordsets:from_list(Primaries))}),
    ets:insert(?META_DB, {replicas, ordsets:to_list(
                                      ordsets:from_list(Replicas))}),
    {reply, ok, State};

handle_call(get_debug_state, _From, State) ->
    {reply, {ok, ets:tab2list(?LOCK_DB),
             ets:tab2list(?DB),
             State#state.lease_expire_ref,
             State#state.write_locks_expire_ref,
             State#state.push_trans_log_ref}, State}.

%%
%% REPLICATION
%%

handle_cast({trans_log, _FromNode, TransLog}, State0) ->
    %% Replay transaction log.

    %% In the future, we might want to offset the lease length in the
    %% master before writing it to the log to ensure the lease length
    %% is at least reasonably similar for all replicas.
    Now = now_to_ms(),
    ReplayF =
        fun ({write, Key, Value, LeaseLength}, State) ->
                %% With multiple masters, we will get multiple writes
                %% for the same key. The last write will win for the
                %% lease db, but make sure we only have one entry in the
                %% expire table.
                delete_expire(expires(Key), Key),

                ExpireAt = expire_at(LeaseLength),
                ets:insert(?DB, {Key, Value, ExpireAt}),
                schedule_expire(ExpireAt, Key),

                NewWaiters = notify_lock_waiter(Now, Key, Value,
                                                State#state.waiters),
                State#state{waiters = NewWaiters};

            ({extend_lease, Key, Value, ExtendLength}, State) ->
                delete_expire(expires(Key), Key),

                ExpireAt = expire_at(ExtendLength),
                ets:insert(?DB, {Key, Value, ExpireAt}),
                schedule_expire(ExpireAt, Key),

                State;

          ({release, Key}, State) ->
              %% Due to replication lag, the key might already have
              %% been expired in which case we simply do nothing
              case ets:lookup(?DB, Key) of
                  [{Key, _Value, ExpireAt}] ->
                      delete_expire(ExpireAt, Key),
                      ets:delete(?DB, Key);
                  [] ->
                      ok
              end,
                State;

            ({update, Key, Value}, State) ->
                delete_expire(expires(Key), Key),

                case ets:lookup(?DB, Key) of
                    [{Key, _Value, ExpireAt}] ->
                        ets:insert(?DB, {Key, Value, ExpireAt}),
                        %% If removal of expired locks and updates were handled
                        %% by multiple processes, i.e. in non-sequential order,
                        %% then it would be possible to end up in a situation,
                        %% in which expired lock has been re-inserted. Calling
                        %% scedule_expire/2 after updating the lock prevents
                        %% from that.
                        schedule_expire(ExpireAt, Key);
                    [] ->
                        %% Lock has been expired
                        ok
                end,

                State

        end,

    NewState = lists:foldl(ReplayF, State0, TransLog),

    {noreply, NewState};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

%%
%% SYSTEM EVENTS
%%

handle_info(expire_leases, State) ->
    %% Delete any leases that has expired. There might be writes in
    %% flight, but they have already been validated in the locking
    %% phase and will be written regardless of what is in the db.

    Now = now_to_seconds(),
    Expired = lists:flatmap(fun (T) -> ets:lookup(?EXPIRE_DB, T) end,
                            lists:seq(State#state.prev_expire_point, Now)),

    ReleaseLockAndNotifyWaiters =
        fun ({At, Key}, RemainingWaiters) ->
                delete_expire(At, Key),
                ets:delete(?DB, Key),
                notify_release_waiter(Key, released, RemainingWaiters)
        end,
    NewWaiters = lists:foldl(ReleaseLockAndNotifyWaiters,
                             State#state.release_waiters, Expired),
    {noreply, State#state{prev_expire_point = Now,
                          release_waiters = NewWaiters}};

handle_info(expire_locks, State) ->
    %% Make a table scan of the write locks. There should be very few
    %% (<1000) writes in progress at any time, so a full scan is
    %% ok. Optimize like the leases if needed.
    Now = now_to_seconds(),
    ets:select_delete(?LOCK_DB,
                      [{ {'_', '_', '$1'}, [{'<', '$1', Now}], [true] }]),

    {noreply, State};

handle_info(push_trans_log, #state{trans_log = TransLog} = State) ->
    %% Push transaction log to *all* replicas. With multiple masters,
    %% each replica will receive the same write multiple times.
    Msg = {trans_log, node(), lists:reverse(TransLog)},
    gen_server:abcast(get_meta_ets(replicas), locker, Msg),
    {noreply, State#state{trans_log = []}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Notify waiter on a lock that the lock has been taken.
notify_lock_waiter(Now, Key, Value, AllWaiters) ->
    KeyWaiter = fun ({K, _, _}) when Key =:= K -> true;
                    (_) -> false
                end,
    ReplyIfNotExpired =
        fun ({_, From, Expire}) when Expire > Now ->
                gen_server:reply(From, {ok, Value});
            (_) ->
                ok
        end,
    {KeyWaiters, OtherWaiters} = lists:partition(KeyWaiter, AllWaiters),
    lists:foreach(ReplyIfNotExpired, KeyWaiters),
    OtherWaiters.

%% Notify waiter of a release of a lock, even if it is expired.
notify_release_waiter(Key, Value, AllWaiters) ->
    KeyWaiter = fun ({K, _, _}) when Key =:= K -> true;
                    (_) -> false
                end,
    Reply = fun ({_, From, _Expire}) -> gen_server:reply(From, {ok, Value}) end,
    {KeyWaiters, OtherWaiters} = lists:partition(KeyWaiter, AllWaiters),
    lists:foreach(Reply, KeyWaiters),
    OtherWaiters.

now_to_seconds() ->
    now_to_seconds(os:timestamp()).

now_to_seconds(Now) ->
    {MegaSeconds, Seconds, _} = Now,
    MegaSeconds * 1000000 + Seconds.

now_to_ms() ->
    now_to_ms(os:timestamp()).

now_to_ms({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + MicroSecs div 1000.

ok_responses(Replies) ->
    lists:partition(fun ({_, ok}) -> true;
                        (_)       -> false
                    end, Replies).

%%
%% EXPIRATION
%%

schedule_expire(At, Key) ->
    true = ets:insert(?EXPIRE_DB, {At, Key}),
    ok.

delete_expire(At, Key) ->
    ets:delete_object(?EXPIRE_DB, {At, Key}),
    ok.

expire_at(Length) ->
    trunc(now_to_seconds() + (Length/1000)).

expires(Key) ->
    case ets:lookup(?DB, Key) of
        [{Key, _Value, ExpireAt}] ->
            ExpireAt;
        [] ->
            []
    end.

%%
%% WRITE-LOCKS
%%

is_locked(Key) ->
    ets:match(?LOCK_DB, {Key, '_', '_'}) =/= [].

set_lock(Tag, Key) ->
    ets:insert_new(?LOCK_DB, {Key, Tag, now_to_seconds() + 10}).

del_lock(Tag) ->
    ets:match_delete(?LOCK_DB, {'_', Tag, '_'}).
