-module(locker).

-behaviour(gen_server).

%% API
-export([start_link/2, add_node/1]).

-export([lock/2]).
-export([request_lock/3, commit_lock/4, abort_lock/2]).
-export([get_up_nodes/0, pid/1, get_debug_state/0]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          n,
          w,

          nodes = [],
          up_nodes = [],
          down_nodes = [],
          joining_nodes = [],

          pending = [], %% {tag, key, pid, now}
          db = dict:new(),
          leases = gb_trees:empty(),

          heartbeat_ref,
          lease_ref
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(N, W) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [N, W], []).

get_up_nodes() ->
    gen_server:call(?MODULE, get_up_nodes).

lock(Key, Pid) ->
    error_logger:info_msg("acquiring lock~n"),
    case get_up_nodes() of
        {ok, Nodes, W} ->
            error_logger:info_msg("Nodes: ~p, W: ~p~n", [Nodes, W]),

            {Tag, RequestReplies, _BadNodes} = locker:request_lock(Nodes, Key, Pid),
            error_logger:info_msg("request replies: ~p~n", [RequestReplies]),

            case ok_responses(RequestReplies) of
                OkNodes when length(OkNodes) >= W ->
                    %% Commit on all nodes
                    {CommitReplies, _} = commit_lock(Nodes, Tag, Key, Pid),
                    error_logger:info_msg("commit replies: ~p~n", [CommitReplies]),
                    ok;
                _ ->
                    {AbortReplies, _} = locker:abort_lock(Nodes, Tag),
                    error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
                    {error, no_quorum}
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
            {error, minority}
    end.

ok_responses(Replies) ->
    [R || {_, ok} = R <- Replies].

request_lock(Nodes, Key, Pid) ->
    Tag = make_ref(),
    {Replies, Down} = gen_server:multi_call(Nodes, locker,
                                            {request_lock, Key, Pid, Tag}, 1000),
    {Tag, Replies, Down}.

commit_lock(Nodes, Tag, Key, Pid) ->
    gen_server:multi_call(Nodes, locker, {commit_lock, Tag, Key, Pid}, 1000).

abort_lock(Nodes, Tag) ->
    gen_server:multi_call(Nodes, locker, {abort_lock, Tag}, 1000).

pid(Key) ->
    gen_server:call(?MODULE, {get_pid, Key}).

add_node(Node) ->
    gen_server:call(?MODULE, {add_node, Node}).

extend_lease(Key, Pid) ->
    gen_server:call(?MODULE, {extend_lease, Key, Pid}).

get_debug_state() ->
    gen_server:call(?MODULE, get_debug_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([N, W]) ->
    ok = net_kernel:monitor_nodes(true),

    HeartbeatRef = timer:send_interval(1000, send_heartbeat),
    {ok, #state{n = N, w = W, heartbeat_ref = HeartbeatRef}}.

handle_call(get_up_nodes, _From, #state{up_nodes = Nodes} = State) ->
    {reply, {ok, [node() | Nodes], State#state.w}, State};

handle_call({election_winner, Key, Pid} = Msg, _From, #state{db = Db} = State) ->
    error_logger:info_msg("gossiper: election winner ~p~n", [Msg]),
    {reply, ok, State#state{db = dict:store(Key, Pid, Db)}};

handle_call({request_lock, Key, Pid, Tag}, _From,
            #state{pending = Pending} = State) ->
    %% Phase 1: request lock on acquiring the key later. Should expire
    %% after a short time if no commit or abort is received.

    case dict:find(Key, State#state.db) of
        {ok, Pid} ->
            Reply = {error, already_own_lock},
            {reply, Reply, State};

        {ok, OtherPid} ->
            Reply = {error, already_registered, OtherPid},
            {reply, Reply, State};

        error ->
            case is_key_pending(Key, Pending) of
                true ->
                    {reply, {error, already_pending}, State};
                false ->
                    NewPending = [{Tag, Key, Pid, now()} | Pending],
                    {reply, ok, State#state{pending = NewPending}}
            end
    end;

handle_call({commit_lock, _Tag, Key, Pid}, _From,
            #state{pending = Pending, db = Db} = State) ->
    %% Phase 2: Blindly create the lock if no lock is already set,
    %% assumes that a quorum was reached in Phase 1. Deletes any
    %% pending locks

    not dict:is_key(Key, Db) orelse throw(already_locked),

    NewPending = lists:keydelete(Key, 2, Pending),
    NewDb = dict:store(Key, {Pid, now()}, Db),
    {reply, ok, State#state{pending = NewPending, db = NewDb}};

handle_call({abort_lock, Tag}, _From, #state{pending = Pending} = State) ->
    case lists:keytake(Tag, 1, Pending) of
        {value, {Tag, _Key, _Pid, _}, NewPending} ->
            {reply, ok, State#state{pending = NewPending}};
        false ->
            {reply, {error, pending_expired}, State}
    end;



handle_call({extend_lease, Key, Pid}, _From, #state{db = Db, leases = Leases} = State) ->
    case dict:find(Key, State#state.db) of
        {ok, {Pid, OldExpireTime}} ->
            NewExpireTime = lease_expire_time(),
            NewLeases = insert_lease(NewExpireTime, Key,
                                     delete_lease(OldExpireTime, Key, Leases)),
            NewDb = dict:store(Key, {Pid, NewExpireTime}, Db),
            {reply, ok, State#state{db = NewDb, leases = NewLeases}};

        {ok, _OtherPid} ->
            {reply, {error, not_owner}, State};
        error ->
            ExpireTime = lease_expire_time(),
            NewLeases = insert_lease(ExpireTime, Key, Leases),
            NewDb = dict:store(Key, {Pid, ExpireTime}, Db),

            {reply, ok, State#state{leases = NewLeases, db = NewDb}}
    end;

handle_call({get_pid, Key}, _From, State) ->
    Reply = case dict:find(Key, State#state.db) of
                {ok, {Pid, _}} ->
                    {ok, Pid};
                error ->
                    {error, not_found}
            end,
    {reply, Reply, State};


handle_call(get_debug_state, _From, #state{pending = Pending, db = Db} = State) ->
    {reply, {ok, Pending, dict:to_list(Db)}, State};


handle_call({add_node, Node}, _From, #state{nodes = Nodes, up_nodes = UpNodes} = State) ->
    error_logger:info_msg("adding node ~p~n", [Node]),
    case lists:keymember(Node, 1, Nodes) of
        true ->
            {reply, ok, State};
        false ->
            {reply, ok, State#state{nodes = [{Node, now()} | Nodes],
                                    up_nodes = [Node | UpNodes]}}
    end.


handle_cast({heartbeat, Node}, #state{nodes = Nodes} = State) ->
    NewNodes = lists:keystore(Node, 1, Nodes, {Node, now()}),
    {noreply, State#state{nodes = NewNodes}}.




handle_info(expire_leases, #state{db = Db, leases = Leases} = State) ->
    Now = now_to_seconds(),
    case gb_trees:is_empty(Leases) of
        false ->
            {ExpireTime, Keys, NewLeases} = gb_trees:take_smallest(Leases),

            case ExpireTime < Now of
                true ->
                    error_logger:info_msg("leases expired: ~p~n", [Keys]),
                    NewDb = lists:foldl(
                              fun (Key, D) ->
                                      dict:erase(Key, D)
                              end, Db, Keys),
                    {noreply, State#state{leases = NewLeases, db = NewDb}};
                false ->
                    {noreply, State}
            end;
        true ->
            {noreply, State}
    end;


handle_info(send_heartbeat, State) ->
    gen_server:abcast(nodenames(State#state.nodes), locker, {heartbeat, node()}),
    {noreply, State};

handle_info({nodedown, Node}, #state{up_nodes = UpNodes, down_nodes = DownNodes} = State) ->
    error_logger:info_msg("nodedown: ~p~n", [Node]),
    case lists:keymember(Node, 1, State#state.nodes) of
        true ->
            {noreply, State#state{up_nodes = lists:delete(Node, UpNodes),
                                  down_nodes = lists:umerge([Node], DownNodes)}};
        false ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

now_to_seconds() ->
    {MegaSeconds, Seconds, _} = now(),
    MegaSeconds * 1000000 + Seconds.

nodenames(Nodes) ->
    [N || {N, _} <- Nodes].


insert_lease(ExpireTime, Key, Leases) ->
    case gb_trees:lookup(ExpireTime, Leases) of
        {value, V} ->
            gb_trees:update(ExpireTime, [Key | V], Leases);
        none ->
            gb_trees:insert(ExpireTime, [Key], Leases)
    end.

delete_lease(ExpireTime, Key, Leases) ->
    case gb_trees:lookup(ExpireTime, Leases) of
        {value, Keys} ->
            gb_trees:update(ExpireTime, lists:delete(Key, Keys), Leases);
        none ->
            Leases
    end.

lease_expire_time() ->
    now_to_seconds() + 3.


is_key_pending(Key, P) ->
    lists:keymember(Key, 2, P).

