-module(locker).

-behaviour(gen_server).

%% API
-export([start_link/1, add_node/1]).

-export([lock/2, extend_lease/2]).
-export([request_lock/3, commit_lock/4, abort_lock/2]).
-export([get_nodes/0, pid/1, get_debug_state/0]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          w,
          nodes = [],
          pending = [], %% {tag, key, pid, now}
          db = dict:new(),
          commands = [],
          lease_expire_ref,
          pending_expire_ref
}).

-define(LEASE_LENGTH, 2000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(W) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [W], []).

get_nodes() ->
    gen_server:call(?MODULE, get_nodes).

lock(Key, Pid) ->
    error_logger:info_msg("acquiring lock~n"),
    case get_nodes() of
        {ok, Nodes, W} ->
            error_logger:info_msg("Nodes: ~p, W: ~p~n", [Nodes, W]),

            {Tag, RequestReplies, BadNodes} = request_lock(Nodes, Key, Pid),
            error_logger:info_msg("request replies: ~p~nbadnodes: ~p~n",
                                  [RequestReplies, BadNodes]),

            case ok_responses(RequestReplies) of
                OkNodes when length(OkNodes) >= W ->
                    %% Commit on all nodes
                    {CommitReplies, _} = commit_lock(Nodes, Tag, Key, Pid),
                    error_logger:info_msg("commit replies: ~p~n", [CommitReplies]),
                    ok;
                _ ->
                    {AbortReplies, _} = abort_lock(Nodes, Tag),
                    error_logger:info_msg("abort replies: ~p~n", [AbortReplies]),
                    {error, no_quorum}
            end;

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
    gen_server:call(?MODULE, {add_node, Node, true}).

extend_lease(Key, Pid) ->
    {ok, Nodes, W} = get_nodes(),
    {Replies, _BadNodes} = gen_server:multi_call(Nodes, locker,
                                                 {extend_lease, Key, Pid, 2000}),
    case ok_responses(Replies) of
        N when length(N) >= W ->
            ok;
        _ ->
            {error, not_all_ok}
    end.

get_debug_state() ->
    gen_server:call(?MODULE, get_debug_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([W]) ->
    {ok, LeaseExpireRef} = timer:send_interval(10000, expire_leases),
    {ok, PendingExpireRef} = timer:send_interval(1000, expire_pending),
    {ok, #state{w = W,
                lease_expire_ref = LeaseExpireRef,
                pending_expire_ref = PendingExpireRef}};

init([W, no_expire]) ->
    {ok, #state{w = W, lease_expire_ref = undefined}}.

handle_call(get_nodes, _From, #state{nodes = Nodes} = State) ->
    {reply, {ok, [node() | Nodes], State#state.w}, State};

%%
%% LOCKING
%%

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
                    NewPending = [{Tag, Key, Pid, now_to_ms()} | Pending],
                    {reply, ok, State#state{pending = NewPending}}
            end
    end;

handle_call({commit_lock, _Tag, Key, Pid}, _From,
            #state{pending = Pending, db = Db} = State) ->
    %% Phase 2: Blindly create the lock if no lock is already set,
    %% assumes that a quorum was reached in Phase 1. Deletes any
    %% pending locks

    %% TODO: don't crash the gen_server..
    not dict:is_key(Key, Db) orelse throw(already_locked),

    NewPending = lists:keydelete(Key, 2, Pending),
    NewDb = dict:store(Key, {Pid, now_to_ms(), ?LEASE_LENGTH}, Db),
    {reply, ok, State#state{pending = NewPending, db = NewDb}};

handle_call({abort_lock, Tag}, _From, #state{pending = Pending} = State) ->
    case lists:keytake(Tag, 1, Pending) of
        {value, {Tag, _Key, _Pid, _}, NewPending} ->
            {reply, ok, State#state{pending = NewPending}};
        false ->
            {reply, {error, pending_expired}, State}
    end;


%%
%% LEASES
%%

handle_call({extend_lease, Key, Pid, ExtendLength}, _From,
            #state{db = Db} = State) ->
    case dict:find(Key, State#state.db) of
        {ok, {Pid, StartTime, LeaseLength}} ->
            case is_expired(StartTime, LeaseLength) of
                true ->
                    {reply, {error, already_expired}, State};
                false ->
                    NewDb = dict:store(Key,
                                       {Pid, StartTime, LeaseLength + ExtendLength},
                                       Db),
                    {reply, ok, State#state{db = NewDb}}
            end;

        {ok, _OtherPid} ->
            {reply, {error, not_owner}, State};
        error ->
            {reply, {error, no_key}, State}
    end;

handle_call({get_pid, Key}, _From, State) ->
    Reply = case dict:find(Key, State#state.db) of
                {ok, {Pid, _, _}} ->
                    {ok, Pid};
                error ->
                    {error, not_found}
            end,
    {reply, Reply, State};

handle_call({add_node, Node, Reverse}, _From, #state{nodes = Nodes} = State) ->
    case lists:member(Node, Nodes) of
        true ->
            {reply, ok, State};
        false ->
            case Reverse of
                true ->
                    ok = gen_server:call({locker, Node}, {add_node, node(), false});
                false ->
                    ok
            end,
            {reply, ok, State#state{nodes = [Node | Nodes]}}
    end;


handle_call(get_debug_state, _From, State) ->
    {reply, {ok, State#state.pending,
             dict:to_list(State#state.db),
             State#state.lease_expire_ref,
             State#state.pending_expire_ref}, State}.

handle_cast(_, State) ->
    {stop, badmsg, State}.


handle_info(expire_leases, #state{db = Db} = State) ->
    Now = now_to_ms(),

    Expired = dict:fold(
                fun(Key, {_Pid, StartTime, LeaseLength}, Acc) ->
                        case is_expired(StartTime, LeaseLength, Now) of
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


handle_info(expire_pending, #state{pending = Pending} = State) ->
    Now = now_to_ms(),

    NewPending = [P || {_, _, _, StartTimeMs} = P <- Pending,
                       StartTimeMs + 5000 > Now],

    {noreply, State#state{pending = NewPending}};

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
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

is_key_pending(Key, P) ->
    lists:keymember(Key, 2, P).

is_expired(StartTime, Lease)->
    is_expired(StartTime, Lease, now_to_ms()).
is_expired(StartTime, Lease, NowMs)->
    StartTime + Lease < NowMs.
