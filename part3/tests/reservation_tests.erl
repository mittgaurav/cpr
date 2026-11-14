-module(reservation_tests).
-moduledoc """
- First start the ticket inventory and
create an event with tickets available
- Then, open sales for the given event
- Start the waiting room for the event
- A User joins and is admitted, giving
them a reservation token (receive-end)
- The reservation is created with that
and the Reservation FSM is thus tested

We shall receive the EventId and Token
once the user is admitted.
""".

-include_lib("eunit/include/eunit.hrl").

%% ===========================
%% ====== Server setup =======
%% ===========================
%% Need running both inventory
%% and the waiting room server
with_servers(TestFun) ->
    {ok, InventoryPid} = ticket_inventory:start_link(),
    {ok, EventId}      = ticket_inventory:create_event("AR Rahman", "Wembley", 100),
    ok = ticket_inventory:open_sale(EventId),
    {ok, WaitingRoomPid} = waiting_room:start_link(EventId, 2),

    try
        TestFun(EventId)
    after
        gen_server:stop(WaitingRoomPid),
        gen_server:stop(InventoryPid)
    end.

%% ===========================
%% ========= Status ==========
%% ===========================
status_test() ->
    with_servers(fun(EventId) ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        receive
            {admitted, EventId, Token} ->
                {ok, ReservationId} = reservation:start_link(EventId, user1, Token, 10, 10_000),
                ?assertEqual({ok, reserved}, reservation:status(ReservationId)),
                ok = reservation:cancel(ReservationId)  %% No more
        end
    end).

%% ===========================
%% ========== Payment ========
%% ===========================
pay_test() ->
    with_servers(fun(EventId) ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        receive
            {admitted, EventId, Token} ->
                {ok, ReservationId} = reservation:start_link(EventId, user1, Token, 10, 10_000),

                ?assertEqual(ok,         reservation:pay(ReservationId, #{success => true})),
                ?assertEqual({ok, paid}, reservation:status(ReservationId))
        end
    end).

%% ===========================
%% ========= Complete ========
%% ===========================
complete_test() ->
    with_servers(fun(EventId) ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        receive
            {admitted, EventId, Token} ->
                {ok, ReservationId} = reservation:start_link(EventId, user1, Token, 10, 10_000),
                ok = reservation:pay(ReservationId, #{success => true}),
                {ok, Tickets} = reservation:complete(ReservationId),

                ?assertEqual(10, length(Tickets)),
                ?assertEqual({error, unknown_reservation}, reservation:status(ReservationId))
        end
    end).

%% ===========================
%% ========= Cancel ==========
%% ===========================
cancel_test() ->
    with_servers(fun(EventId) ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        receive
            {admitted, EventId, Token} ->
                {ok, ReservationId} =
                    reservation:start_link(
                        EventId, user1, Token, 10, 10_000
                    ),

                ?assertEqual(ok, reservation:cancel(ReservationId)),
                ?assertEqual({error, unknown_reservation}, reservation:status(ReservationId))
        end
    end).

%% ===========================
%% ===== Invalid Payment =====
%% ===========================
payment_failed_test() ->
    with_servers(fun(EventId) ->
        {ok, queued, 0} = waiting_room:join(EventId, user1, self()),
        {ok, [user1]}   = waiting_room:admit(EventId, 1),

        receive
            {admitted, EventId, Token} ->
                {ok, ReservationId} = reservation:start_link(EventId, user1, Token, 10, 10_000),

                ?assertEqual({error, payment_failed}, reservation:pay(ReservationId, #{success => false})),
                ?assertEqual({ok, reserved},          reservation:status(ReservationId)),
                ok = reservation:cancel(ReservationId)  %% No more

        end
    end).
