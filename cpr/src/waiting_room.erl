%% waiting_room.erl
%%
%% Waiting room management server.

-module(waiting_room).

-moduledoc """
Waiting room server.
APIs to users to join the queue
to use ticket inventory server.

Setup one process per event and
register it in global namespace

EventID is used to find process

Each of data structures: queue,
user/admitted lists, and tokens
are stored in process's memory.
Persisting them is unnecessary.
Also, there is no need to
hence, Putting them in persisted,  see any compelling reason
""".
%% System clocks can be changed!
-define(NOW_MS, erlang:monotonic_time(millisecond)).
-define(TOKEN_EXPIRY_MS, 10 * 60 * 1_000).  %% 10min

-behaviour(gen_server).

%% Public APIs
-export([
    start_link/2,
    join/3,
    position/2,
    leave/2,
    admit/2,
    validate/2
]).

%% Callbacks for `gen_server`
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

%% Test expiry validate
-export([handle_validate/4]).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start server".
-doc #{params => #{"EventId" => "Event Id for event", "AdmissionLimit" => "Max Size of Queueueue"}}.
-spec start_link(reference(), pos_integer()) -> {ok, pid()} | {error, invalid}.
start_link(EventId, AdmissionLimit)
    when is_reference(EventId), is_integer(AdmissionLimit), AdmissionLimit > 0 ->
    case gen_server:start_link({global, {?MODULE, EventId}}, ?MODULE, {EventId, AdmissionLimit}, []) of
        {ok, Pid}  -> {ok, Pid};
        {error, _} -> {error, invalid}
    end;

start_link(_, _) ->
    {error, invalid}.

-doc "Join queue for an event".
-doc #{params => #{"EventId" => "Event Id for event", "UserId" => "User Id", "UserPid" => "Pid to be notified"}}.
%% Allow user id to be opaque with term
-spec join(reference(), term(), pid()) -> {ok, queued, non_neg_integer()} | {error, already_joined}.
join(EventId, UserId, UserPid) ->
    gen_server:call({global, {?MODULE, EventId}}, {join, UserId, UserPid}).

-doc "Get position in queue".
-doc #{params => #{"EventId" => "Event Id for event", "UserId" => "User Id"}}.
-spec position(reference(), term()) -> {ok, non_neg_integer()} | {error, unknown_user | admitted}.
position(EventId, UserId) ->
    gen_server:call({global, {?MODULE, EventId}}, {position, UserId}).

-doc "Leave the queue".
-doc #{params => #{"EventId" => "Event Id for event", "UserId" => "User Id"}}.
-spec leave(reference(), term()) -> ok | {error, unknown_user}.
leave(EventId, UserId) ->
    gen_server:call({global, {?MODULE, EventId}}, {leave, UserId}).

-doc "Admit given users from queue and notify them".
-doc #{params => #{"EventId" => "Event Id for event", "Count" => "Num of users to admit"}}.
-spec admit(reference(), pos_integer()) -> {ok, [term()]} | {error, invalid}.
admit(EventId, Count) ->
    gen_server:call({global, {?MODULE, EventId}}, {admit, Count}).

-doc "Validate token is valid and not expired".
-doc #{params => #{"EventId" => "Event Id for event", "Token" => "Token given to admitted users"}}.
-spec validate(reference(), reference()) -> ok | {error, invalid | expired}.
%% Should we verify user too?
validate(EventId, Token) ->
    gen_server:call({global, {?MODULE, EventId}}, {validate, Token}).

%% ============================
%% ======== Callbacks =========
%% ============================
%% Initialize server with empty
%% waiting queue state as below
%%
%% event_id        => reference()
%% admission_limit => pos_integer()
%% queue           => [UserId]
%% queue_size      => pos_integer()
%% users           => #{UserId => UserPid}
%% admitted        => #{UserId => true}
%% tokens          => #{Token  => {UserId, ExpiryTime(ms)}}
%%
%% `admitted` required for already_joined
%% `tokens`   required for tokens expired
init({EventId, AdmissionLimit}) ->
    logger:info("Initialized waiting room for ~p", [EventId]),
    {ok, #{
        %% should store queue size to
        %% optimize queue operations?
        event_id        => EventId,
        admission_limit => AdmissionLimit,
        queue           => [],
        users           => #{},
        admitted        => #{},
        tokens          => #{}
    }}.

%% ========== Join ============
%% User is already in the queue
%% or has already been admitted
handle_call({join, UserId, _UserPid}, _From, State = #{users := Users, admitted := Admitted})
    when is_map_key(UserId, Users); is_map_key(UserId, Admitted) ->
    %% Can we differentiate between
    %% admitted and in queue users?
    {reply, {error, already_joined}, State};

%% Add the new user to the end
handle_call({join, UserId, UserPid}, _From, State = #{queue := Queue, users := Users}) ->
    Position = length(Queue),

    %% Add at the end of FIFO
    %% O(n), as must traverse
    %% entire queue to add one
    NewQueue = [UserId | Queue],
    NewUsers = Users#{UserId => UserPid},

    NewState = State#{
        queue := NewQueue,
        users := NewUsers
    },

    logger:info("join: User ~p joined from ~p", [UserId, UserPid]),
    {reply, {ok, queued, Position}, NewState};

%% ======== Position =========
%% User already admitted error
handle_call({position, UserId}, _From, State = #{admitted := Admitted})
    when is_map_key(UserId, Admitted) ->
    {reply, {error, admitted}, State};

%% Give user position in queue
handle_call({position, UserId}, _From, State = #{queue := Queue, users := Users})
    when is_map_key(UserId, Users) ->
    {reply, {ok, get_position(UserId, Queue)}, State};

handle_call({position, _UserId}, _From, State) ->
    {reply, {error, unknown_user}, State};

%% ========== Leave ==========
%% Admitted users can't leave.
handle_call({leave, UserId}, _From, State = #{admitted := Admitted})
    when is_map_key(UserId, Admitted) ->
    {reply, {error, unknown_user}, State};

%% Let a user leave the queue.
%% We are not Hotel California
handle_call({leave, UserId}, _From, State = #{queue := Queue, users := Users})
    when is_map_key(UserId, Users) ->
    NewQueue = lists:delete(UserId, Queue),
    NewUsers = maps:remove(UserId, Users),

    NewState = State#{
        queue := NewQueue,
        users := NewUsers
    },
    logger:info("leave: User ~p left :(", [UserId]),
    {reply, ok, NewState};

%% When user is already admitted
%% there we also give same error
handle_call({leave, _UserId}, _From, State) ->
    {reply, {error, unknown_user}, State};

%% ========== Admit ==========
%% Count shall be positive int
handle_call({admit, Count}, _From, State)
    when not is_integer(Count); Count =< 0 ->
    {reply, {error, invalid}, State};

%% Admit users in queue order
%% as per "upto" Count can be
handle_call({admit, Count}, _From, State = #{queue           := Queue,
                                             users           := Users,
                                             admitted        := Admitted,
                                             tokens          := Tokens,
                                             admission_limit := AdmissionLimit}) ->
    %% Don't admit any more
    %% than admission limit
    %% or the size of queue
    %% (the admission limit
    %% that's available)
    Length      = length(Queue),
    AdmittedLen = maps:size(Admitted),
    Available   = max(0, AdmissionLimit - AdmittedLen),
    ActualCount = min(Count, min(Available, Length)),
    logger:debug("count: ~p, available: ~p, Q-len: ~p, actual: ~p", [Count, Available, Length, ActualCount]),

    %% Split the user queue
    {NewQueue, ToBeAdmitted} = lists:split(Length - ActualCount, Queue),
    ToBeAdmitted = lists:reverse(ToBeAdmitted),

    NewAdmitted = add_admitted(ToBeAdmitted, Admitted),
    NewTokens   = notify_admit(ToBeAdmitted, Users, maps:get(event_id, State), Tokens),

    %% Keep user pid for
    %% old friends' sake
    NewState    = State#{
        queue    := NewQueue,
        admitted := NewAdmitted,
        tokens   := NewTokens
    },
    logger:info("admit: Admitted ~p users. New queue size ~p", [ActualCount, Length - ActualCount]),
    {reply, {ok, ToBeAdmitted}, NewState};

%% ========= Validate =========
%% Validate token isn't expired
handle_call({validate, Token}, _From, State = #{tokens := Tokens})
    when is_map_key(Token, Tokens) ->
    handle_validate(Token, State, ?NOW_MS, maps:get(Token, Tokens));

handle_call({validate, _Token}, _From, State) ->
    {reply, {error, invalid}, State};

%% Ignore unmatched
handle_call(_, _From, State) ->
    {reply, {error, unknown_api}, State}.

%% Satisfy Sir General Server
handle_cast(_, State) ->
    {noreply, State}.

%% ===========================
%% ======== Internal =========
%% ===========================
%% Find the position of a user
%% Seems lists doesn't have it
get_position(UserId, Queue) ->
    get_position(UserId, Queue, length(Queue) - 1).

%% Tail recursion using an acc
get_position(UserId, [UserId | _], Position) ->
    Position;

%% Not first find them in tail
get_position(UserId, [_ | Tail], Position) ->
    get_position(UserId, Tail, Position - 1);

%% Shouldn't really be reached
get_position(_UserId, [], _Position) ->
    not_found.

%% Admit list of users to map
add_admitted([], Admitted) ->
    Admitted;

add_admitted([UserId | Tail], Admitted) ->
    add_admitted(Tail, Admitted#{UserId => true}).

%% Create a token and notify
%% admitted user's processid
notify_admit([], _Users, _EventId, Tokens) ->
    Tokens;

notify_admit([UserId | Tail], Users, EventId, Tokens) ->
    UserPid = maps:get(UserId, Users),

    %% Generate token
    Token   = make_ref(),
    Expiry  = ?NOW_MS + (?TOKEN_EXPIRY_MS),

    %% Notify admitted user
    UserPid ! {admitted, EventId, Token},

    notify_admit(Tail, Users, EventId, Tokens#{Token => {UserId, Expiry}}).

%% ========= Validate =========
handle_validate(Token, State = #{tokens := Tokens, admitted := Admitted}, Now, {UserId, Expiry})
    when Now < Expiry ->
    %% Remove Token to prevent reuse.
    %% Free Admitted after user leave
    NewState = State#{
        tokens   := maps:remove(Token,  Tokens),
        admitted := maps:remove(UserId, Admitted)
    },
    {reply, ok, NewState};

handle_validate(Token, State = #{tokens := Tokens, admitted := Admitted}, _Now, {UserId, _Expiry}) ->
    logger:warning("validate: Token ~p from ~p expired", [Token, UserId]),
    NewState = State#{
        tokens   := maps:remove(Token,  Tokens),
        admitted := maps:remove(UserId, Admitted)
    },
    {reply, {error, expired}, NewState}.
