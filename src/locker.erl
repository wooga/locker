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
-export([start_link/1, add_node/1, remove_node/1, set_w/1, join/1]).

-export([lock/2, lock/3, extend_lease/3, release/2]).
-export([get_write_lock/3, do_write/5, release_write_lock/2]).
-export([get_nodes/0, pid/1, get_debug_state/0]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          %% w is the number of nodes required to make the write
          %% quorum, this must be set manually for now as there is no
          %% liveness checking of nodes and the introduction of a new
          %% node is still very much a manual process
          w,

          %% Nodes in the 'locker' cluster, where all writes should be sent
          nodes = [],

          %% The in memory database. For now it is a dict, if we want
          %% higher performance, we could use an ETS table
          db = dict:new(),

          %% A list of write-locks, only one lock per key is allowed,
          %% but a list is used for performance as there will be few
          %% concurrent locks
          locks = [], %% {tag, key, pid, now}

          %% A boolean telling us if the node currently blocks
          %% writes. When a new node is added to the cluster,
          %% write-locks are not granted and writes are stopped.
          allow_writes = false,

          %% Timer references
          lease_expire_ref,
          write_locks_expire_ref
}).

-define(LEASE_LENGTH, 2000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(W) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [W], []).

get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

set_w(W) when is_integer(W) ->
    gen_server:call(?MODULE, {set_w, W}).

lock(Key, Pid) ->
    lock(Key, Pid, ?LEASE_LENGTH).

%% @doc: Tries to acquire the lock. In case of unreachable nodes, the
%% timeout is 1 second per node which might need tuning. Returns {ok,
%% W, V, C} where W is the number of agreeing nodes required for a
%% quorum, V is the number of nodes that voted in favor of this lock
%% in the case of contention and C is the number of nodes who
%% acknowledged commit of the lock successfully.
lock(Key, Value, LeaseLength) ->
    {ok, Nodes, W} = get_nodes(),
    error_logger:info_msg("Nodes: ~p, W: ~p~n", [Nodes, W]),

    %% Try getting the write lock on all nodes
    {Tag, RequestReplies, BadNodes} = get_write_lock(Nodes, Key, not_found),
    error_logger:info_msg("request replies: ~p~nbadnodes: ~p~n",
                          [RequestReplies, BadNodes]),

    case ok_responses(RequestReplies) of
        {OkNodes, _ErrorNodes} when length(OkNodes) >= W ->
            %% Majority of nodes gave us the lock, go ahead and do the
            %% write on all nodes. The write also releases the lock

            {WriteReplies, _} = do_write(Nodes, Tag, Key, Value, LeaseLength),
            {OkWrites, _BadWrites} = ok_responses(WriteReplies),
            error_logger:info_msg("write replies: ~p~n", [WriteReplies]),
            {ok, W, length(OkNodes), length(OkWrites)};
        _ ->
            {AbortReplies, _} = release_write_lock(Nodes, Tag),
            error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
            {error, no_quorum}
    end.

release(Key, Value) ->
    error_logger:info_msg("releasing lock~n"),
    {ok, Nodes, W} = get_nodes(),

    %% Try getting the write lock on all nodes
    {Tag, WriteLockReplies, BadNodes} = get_write_lock(Nodes, Key, Value),
    error_logger:info_msg("write lock replies: ~p~nbadnodes: ~p~n",
                          [WriteLockReplies, BadNodes]),

    case ok_responses(WriteLockReplies) of
        {OkNodes, _ErrorNodes} when length(OkNodes) >= W ->
            Request = {release, Key, Value, Tag},
            {ReleaseReplies, _BadNodes} =
                gen_server:multi_call(Nodes, locker, Request, 1000),

            error_logger:info_msg("release replies: ~p~n", [ReleaseReplies]),
            {OkWrites, _BadWrites} = ok_responses(ReleaseReplies),

            {ok, W, length(OkNodes), length(OkWrites)};
        _ ->
            {AbortReplies, _} = release_write_lock(Nodes, Tag),
            error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
            {error, no_quorum}
    end.

%% @doc: Extends the lease for the lock on all nodes that are up. What
%% really happens is that the expiration is scheduled for (now + lease
%% time), to allow for nodes that just joined to set the correct
%% expiration time without knowing the start time of the lease.
extend_lease(Key, Value, LeaseTime) ->
    {ok, Nodes, W} = get_nodes(),
    {Tag, WriteLockReplies, BadNodes} = get_write_lock(Nodes, Key, Value),
    error_logger:info_msg("write lock replies: ~p~nbadnodes: ~p~n",
                          [WriteLockReplies, BadNodes]),

    case ok_responses(WriteLockReplies) of
        {N, E} when length(N) >= W ->
            error_logger:info_msg("lock replies: ~p, ~p~n", [N, E]),

            Request = {extend_lease, Tag, Key, Value, LeaseTime},
            {Replies, _BadNodes} = gen_server:multi_call(Nodes, locker, Request, 1000),
            {_, FailedExtended} = ok_responses(Replies),
            error_logger:info_msg("extend replies: ~p~n", [Replies]),
            release_write_lock(FailedExtended, Tag),
            ok;
        _ ->
            {AbortReplies, _} = release_write_lock(Nodes, Tag),
            error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
            {error, majority_not_ok}
    end.


get_write_lock(Nodes, Key, Value) ->
    Tag = make_ref(),
    Request = {get_write_lock, Key, Value, Tag},
    {Replies, Down} = gen_server:multi_call(Nodes, locker, Request, 1000),
    {Tag, Replies, Down}.

do_write(Nodes, Tag, Key, Value, LeaseLength) ->
    gen_server:multi_call(Nodes, locker, {write, Tag, Key, Value, LeaseLength}, 1000).


release_write_lock(Nodes, Tag) ->
    gen_server:multi_call(Nodes, locker, {release_write_lock, Tag}, 1000).

pid(Key) ->
    gen_server:call(?MODULE, {get_pid, Key}).

%% @doc: Connects the current node with the cluster, forming a fully
%% connected cluster where the local node copies the db from the seed
%% node. This operation will block all writes in the entire cluster
%% during synchronization.
join(SeedNode) ->
    {ok, Nodes, _} = gen_server:call({?MODULE, SeedNode}, get_nodes, 500),
    error_logger:info_msg("nodes according to seed: ~p~n", [Nodes]),
    AllNodes = [node() | Nodes],

    {BlockReplies, []} = gen_server:multi_call(AllNodes, locker, block_writes),
    error_logger:info_msg("block replies: ~p~n", [BlockReplies]),

    %% wait for write locks to be released
    {CountReplies, []} = gen_server:multi_call(AllNodes, locker, num_write_locks),

    EmptyNodes = lists:flatmap(fun ({N, {ok, 0}}) -> [N];
                                   (_)            -> []
                              end, CountReplies),
    error_logger:info_msg("lock counts: ~p~n", [CountReplies]),
    error_logger:info_msg("empty: ~p, all: ~p~n", [EmptyNodes, AllNodes]),
    case lists:sort(EmptyNodes) =:= lists:sort(AllNodes) of
        true ->
            {ok, Db} = gen_server:call({?MODULE, SeedNode}, get_db, 500),
            ok = gen_server:call(?MODULE, {replace_db, Db}, 500),

            {AddNodeReplies, []} = gen_server:multi_call(Nodes, locker, {add_nodes, [node()]}),
            error_logger:info_msg("add node replies: ~p~n", [AddNodeReplies]),
            ok = gen_server:call(?MODULE, {add_nodes, Nodes}),

            %% Update the write quorum

            %% Allow writes

            {AllowReplies, []} = gen_server:multi_call(AllNodes, locker, allow_writes),
            error_logger:info_msg("allow replies: ~p~n", [AllowReplies]),

            ok;
        _ ->
            {error, {write_locks_remaining, AllNodes -- EmptyNodes}}
    end.



add_node(Node) ->
    gen_server:call(?MODULE, {add_nodes, [Node]}).

remove_node(Node) ->
    gen_server:call(?MODULE, {remove_node, Node}).



get_debug_state() ->
    gen_server:call(?MODULE, get_debug_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([W]) ->
    {ok, LeaseExpireRef} = timer:send_interval(10000, expire_leases),
    {ok, WriteLocksExpireRef} = timer:send_interval(1000, expire_locks),
    {ok, #state{w = W,
                nodes = ordsets:new(),
                lease_expire_ref = LeaseExpireRef,
                write_locks_expire_ref = WriteLocksExpireRef}}.


%%
%% WRITE-LOCKS
%%

handle_call({get_write_lock, _, _, _}, _From, #state{allow_writes = false} = State) ->
    {reply, {error, writes_not_allowed}, State};

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
            case dict:find(Key, State#state.db) of
                {ok, {DbValue, _}} when DbValue =:= Value ->
                    NewLocks = [{Tag, Key, Value, now_to_ms()} | Locks],
                    {reply, ok, State#state{locks = NewLocks}};
                error when Value =:= not_found->
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


handle_call({write, _, _, _, _}, _From, #state{allow_writes = false} = State) ->
    {reply, {error, writes_not_allowed}, State};

handle_call({write, LockTag, Key, Value, LeaseLength}, _From,
            #state{locks = Locks, db = Db} = State) ->
    %% Database write. LockTag might be a valid write-lock, in which
    %% case it is deleted to avoid the extra round-trip of explicit
    %% delete. If it is not valid, we assume the coordinator had a
    %% quorum before writing.

    NewLocks = lists:keydelete(LockTag, 1, Locks),
    NewDb = dict:store(Key, {Value, now_to_ms() + LeaseLength}, Db),
    {reply, ok, State#state{locks = NewLocks, db = NewDb}};


handle_call(get_db, _From, State) ->
    {reply, {ok, dict:to_list(State#state.db)}, State};

handle_call({replace_db, Db}, _From, State) ->
    {reply, ok, State#state{db = dict:from_list(Db)}};


%%
%% LEASES
%%

handle_call({extend_lease, LockTag, Key, Value, ExtendLength}, _From,
            #state{locks = Locks, db = Db} = State) ->
    %% Extending a lease is a special case of a write with extra
    %% validation. It also assumes a quorum in the coordinator. When a
    %% lease is extended, it is gossiped to the other members to allow
    %% new nodes to get up to speed on existing leases.

    case dict:find(Key, State#state.db) of
        {ok, {Value, ExpireTime}} ->
            case is_expired(ExpireTime) of
                true ->
                    {reply, {error, already_expired}, State};
                false ->
                    gen_server:abcast(State#state.nodes, locker,
                                      {extended_lease, node(), Key, Value, ExtendLength}),

                    NewLocks = lists:keydelete(LockTag, 1, Locks),
                    NewDb = dict:store(Key, {Value, now_to_ms() + ExtendLength},
                                       Db),
                    {reply, ok, State#state{locks = NewLocks, db = NewDb}}
            end;

        {ok, _OtherValue} ->
            {reply, {error, not_owner}, State};
        error ->
            {reply, {error, not_found}, State}
    end;


handle_call({release, Key, Value, LockTag}, _From,
            #state{locks = Locks, db = Db} = State) ->
    case dict:find(Key, State#state.db) of
        {ok, {Value, _}} ->
            NewLocks = lists:keydelete(LockTag, 1, Locks),
            NewDb = dict:erase(Key, Db),
            {reply, ok, State#state{locks = NewLocks, db = NewDb}};

        {ok, {_OtherPid, _}} ->
            {reply, {error, not_owner}, State};
        error ->
            {reply, {error, not_found}, State}
    end;




%%
%% ADMINISTRATION
%%

handle_call(block_writes, _From, State) ->
    {reply, ok, State#state{allow_writes = false}};

handle_call(allow_writes, _From, State) ->
    {reply, ok, State#state{allow_writes = true}};

handle_call(num_write_locks, _From, State) ->
    {reply, {ok, length(State#state.locks)}, State};


handle_call(get_nodes, _From, #state{nodes = Nodes} = State) ->
    {reply, {ok, [node() | Nodes], State#state.w}, State};

handle_call({set_w, W}, _From, State) ->
    {reply, ok, State#state{w = W}};

handle_call({get_pid, Key}, _From, State) ->
    Reply = case dict:find(Key, State#state.db) of
                {ok, {Pid, _}} ->
                    {ok, Pid};
                error ->
                    {error, not_found}
            end,
    {reply, Reply, State};

handle_call({add_nodes, Ns}, _From, #state{nodes = Nodes} = State) ->
    NewNodes = lists:foldl(fun ordsets:add_element/2, Nodes, Ns),
    {reply, ok, State#state{nodes = NewNodes}};

handle_call({remove_node, Node}, _From,
            #state{nodes = Nodes} = State) ->
    NewNodes = ordsets:del_element(Node, Nodes),
    {reply, ok, State#state{nodes = NewNodes}};


handle_call(get_debug_state, _From, State) ->
    {reply, {ok, State#state.locks,
             dict:to_list(State#state.db),
             State#state.lease_expire_ref,
             State#state.write_locks_expire_ref}, State}.



%%
%% ASYNCHRONOUS LOCKER-TO-LOCKER
%%

handle_cast({extended_lease, FromNode, Key, Pid, ExtendLength}, #state{db = Db} = State) ->
    error_logger:info_msg("got gossip from ~p: extend lease ~p~n", [FromNode, Key]),
    case dict:is_key(Key, Db) of
        true ->
            {noreply, State};
        false ->
            NewDb = dict:store(Key, {Pid, now_to_ms() + ExtendLength}, Db),
            {noreply, State#state{db = NewDb}}
    end.



handle_info(expire_leases, #state{db = Db} = State) ->
    Now = now_to_ms(),
    Expired = dict:fold(
                fun(Key, {_Pid, ExpireTime}, Acc) ->
                        case is_expired(ExpireTime, Now) of
                            true ->
                                [Key | Acc];
                            false ->
                                Acc
                        end
                end, [], Db),

    NewDb = lists:foldl(fun (Key, D) ->
                                dict:erase(Key, D)
                        end, Db, Expired),

    {noreply, State#state{db = NewDb}};


handle_info(expire_locks, #state{locks = Locks} = State) ->
    Now = now_to_ms(),
    NewLocks = [L || {_, _, StartTimeMs} = L <- Locks, StartTimeMs + 1000 > Now],
    {noreply, State#state{locks = NewLocks}};

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

is_expired(StartTime)->
    is_expired(StartTime, now_to_ms()).

is_expired(ExpireTime, NowMs)->
    ExpireTime < NowMs.

ok_responses(Replies) ->
    lists:partition(fun ({_, ok}) -> true;
                        (_)        -> false
                    end, Replies).
