-module(waiting_room_tests).

-include_lib("eunit/include/eunit.hrl").

%% ===========================
%% ====== Server setup =======
%% ===========================
with_server(EventId, TestFun) ->
    {ok, Pid} = waiting_room:start_link(EventId, 2),
    try
        TestFun()
    after
        gen_server:stop(Pid)
    end.

%% ===========================
%% ========= Server ==========
%% ===========================
start_link_test() ->
    EventId = make_ref(),

    {ok, Pid} = waiting_room:start_link(EventId, 2),
    ?assert(is_process_alive(Pid)),

    %% Event cant have another
    %% waiting room registered
    ?assertEqual({error, invalid}, waiting_room:start_link(EventId, 2)),
    gen_server:stop(Pid).


start_link_invalid_test() ->
    ?assertEqual({error, invalid}, waiting_room:start_link(make_ref(), 0)),
    ?assertEqual({error, invalid}, waiting_room:start_link(make_ref(), -1)),
    ?assertEqual({error, invalid}, waiting_room:start_link(make_ref(), gaga)),
    ?assertEqual({error, invalid}, waiting_room:start_link(gaga, 2)).

%% ===========================
%% ========== Join ===========
%% ===========================
join_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        ?assertEqual({ok, queued, 0},         waiting_room:join(EventId, user1, self())),
        ?assertEqual({ok, queued, 1},         waiting_room:join(EventId, user2, self())),
        ?assertEqual({ok, 0},                 waiting_room:position(EventId, user1)),
        ?assertEqual({ok, 1},                 waiting_room:position(EventId, user2)),

        %% No can join again
        ?assertEqual({error, already_joined}, waiting_room:join(EventId, user1, self()))
    end).

%% ===========================
%% ======== Position =========
%% ===========================
position_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        ?assertEqual({error, unknown_user}, waiting_room:position(EventId, unknown_user)),

        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]} = waiting_room:admit(EventId, 1),

        %% Admitted users do not
        %% have a queue position
        ?assertEqual({error, admitted},     waiting_room:position(EventId, user1))
    end).

%% ===========================
%% ========== Leave ==========
%% ===========================
leave_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, queued, 1} = waiting_room:join(EventId, user2, self()),

        ok = waiting_room:leave(EventId, user1),
        ?assertEqual({error, unknown_user}, waiting_room:position(EventId, user1)),

        %% user2's patience is a virtue
        %% and their results are sweet!
        ?assertEqual({ok, 0},               waiting_room:position(EventId, user2))
    end).

leave_unknown_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        ?assertEqual({error, unknown_user}, waiting_room:leave(EventId, unknown_user)),

        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        %% No money back; admitted
        %% users can't leave queue
        ?assertEqual({error, unknown_user}, waiting_room:leave(EventId, user1))
    end).

%% ===========================
%% ========== Admit ==========
%% ===========================
admit_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        %% Add Two users
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, queued, 1} = waiting_room:join(EventId, user2, self()),
        %% Admit one user
        ?assertEqual({ok, [user1]}, waiting_room:admit(EventId, 1)),

        %% Waiting room sends message
        %% {admitted, EventId, Token}
        %% Verify that we received it
        receive
            {admitted, EventId, Token} ->
                ?assert(is_reference(Token)),
                %% Let's validate
                ?assertEqual(ok, waiting_room:validate(EventId, Token))
        end,

        %% Second is bumped up
        ?assertEqual({ok, 0}, waiting_room:position(EventId, user2))
    end).


admit_order_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, queued, 1} = waiting_room:join(EventId, user2, self()),

        %% Admitted in queue order
        ?assertEqual({ok, [user1, user2]}, waiting_room:admit(EventId, 2))
    end).


admit_limit_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, queued, 1} = waiting_room:join(EventId, user2, self()),

        %% AdmissionLimit is 2 - A large
        %% count can't admit more than 2
        ?assertEqual({ok, [user1, user2]}, waiting_room:admit(EventId, 100))
    end).

admit_invalid_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        ?assertEqual({error, invalid}, waiting_room:admit(EventId, 0)),
        ?assertEqual({error, invalid}, waiting_room:admit(EventId, -1)),
        ?assertEqual({error, invalid}, waiting_room:admit(EventId, gaga))
    end).

admit_empty_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        %% No users waiting
        %% Lets don't crash
        ?assertEqual({ok, []},      waiting_room:admit(EventId, 2))
    end).

admit_same_user_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        ?assertEqual({ok, [user1]}, waiting_room:admit(EventId, 1)),

        %% No BornAgain here
        %% Already admitted!
        ?assertEqual({ok, []},      waiting_room:admit(EventId, 1))
    end).

%% ===========================
%% ========= Validate ========
%% ===========================
validate_invalid_test() ->
    EventId = make_ref(),

    with_server(EventId, fun() ->
        ?assertEqual({error, invalid}, waiting_room:validate(EventId, make_ref()))
    end).

validate_valid_test() ->
    EventId = make_ref(),
    Token   = make_ref(),

    State   = #{
        event_id        => EventId,
        admission_limit => 2,
        queue           => [],
        users           => #{},
        admitted        => #{},
        tokens          => #{
            Token => {user1, 200}
        }
    },

    ExpectedState = State#{
        tokens   := #{},
        admitted := #{}
    },
    ?assertEqual({reply, ok, ExpectedState}, waiting_room:handle_validate(Token, State, 100, {user1, 200})).

validate_expired_test() ->
    EventId = make_ref(),
    Token   = make_ref(),

    %% Call the internal validation function
    %% directly with an already-expired time
    State   = #{
        event_id        => EventId,
        admission_limit => 2,
        queue           => [],
        users           => #{},
        admitted        => #{},
        tokens          => #{
            Token => {user1, 100}
        }
    },

    ExpectedState = State#{
        tokens   := #{},
        admitted := #{}
    },
    ?assertEqual({reply, {error, expired}, ExpectedState}, waiting_room:handle_validate(Token, State, 101, {user1, 100})).
