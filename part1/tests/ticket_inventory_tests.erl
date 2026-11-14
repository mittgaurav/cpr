-module(ticket_inventory_tests).

-include_lib("eunit/include/eunit.hrl").
-include("event.hrl").
-include("reservation.hrl").

%% ===========================
%% ====== Server setup =======
%% ===========================
with_server(TestFun) ->
    {ok, Pid} = ticket_inventory:start_link(),
    try
        TestFun()
    after
        gen_server:stop(Pid)
    end.

%% ===========================
%% ========= Server ==========
%% ===========================
start_link_test() ->
    with_server(fun() ->
        ?assert(is_process_alive(whereis(ticket_inventory))),
        ?assertEqual({error, already_started}, ticket_inventory:start_link())
    end).

%% ===========================
%% ====== Create Event =======
%% ===========================
create_event_test() ->
    with_server(fun() ->
        {ok, Ariana} = ticket_inventory:create_event("Ariana Grande", "O2", 100),
        {ok, Event}  = ticket_inventory:event_info(Ariana),

        ?assertEqual("Ariana Grande", Event#event.name),
        ?assertEqual("O2",            Event#event.venue),
        ?assertEqual(100,             Event#event.total_tickets),
        ?assertEqual(100,             Event#event.available_tickets),
        ?assertEqual(0,               Event#event.reserved_tickets),
        ?assertEqual(not_on_sale,     Event#event.status)
    end).


create_event_invalid_test() ->
    with_server(fun() ->
        %% invalid Name/Venue
        ?assertEqual({error, invalid}, ticket_inventory:create_event([], "O2", 100)),
        ?assertEqual({error, invalid}, ticket_inventory:create_event("Ariana Grande", [], 100)),

        %% invalid quantities
        ?assertEqual({error, invalid}, ticket_inventory:create_event("Ariana Grande", "O2", 0)),
        ?assertEqual({error, invalid}, ticket_inventory:create_event("Ariana Grande", "O2", -1))
    end).


%% ===========================
%% ====== Unknown Event ======
%% ===========================
unknown_event_test() ->
    with_server(fun() ->
        UnknownEvent = make_ref(),

        ?assertEqual({error, unknown_event}, ticket_inventory:event_info(UnknownEvent)),
        ?assertEqual({error, unknown_event}, ticket_inventory:available(UnknownEvent)),
        ?assertEqual({error, unknown_event}, ticket_inventory:open_sale(UnknownEvent)),
        ?assertEqual({error, unknown_event}, ticket_inventory:reserve(UnknownEvent, 1)),
        ?assertEqual({error, unknown_event}, ticket_inventory:release(UnknownEvent,make_ref()))
    end).

%% ===========================
%% ======= Open Event ========
%% ===========================
open_sale_test() ->
    with_server(fun() ->
        {ok, Ariana} = ticket_inventory:create_event("Ariana Grande", "O2", 100),

        ?assertEqual(ok,                    ticket_inventory:open_sale(Ariana)),
        ?assertEqual({error, already_open}, ticket_inventory:open_sale(Ariana))
    end).

%% ===========================
%% ========= Reserve =========
%% ===========================
reserve_errors_test() ->
    with_server(fun() ->
        {ok, ARRahman} = ticket_inventory:create_event("AR Rahman", "Wembley", 100),

        %% Sale has not opened.
        ?assertEqual({error, not_on_sale},          ticket_inventory:reserve(ARRahman, 10)),
        ok = ticket_inventory:open_sale(ARRahman),

        ?assertEqual({error, unknown_reservation},  ticket_inventory:reservation_info(ARRahman, make_ref())),

        %% Invalid
        ?assertEqual({error, invalid_quantity},     ticket_inventory:reserve(ARRahman, 0)),
        ?assertEqual({error, invalid_quantity},     ticket_inventory:reserve(ARRahman, -1)),
        ?assertEqual({error, invalid_quantity},     ticket_inventory:reserve(ARRahman, gaga)),

        %% No Overbooking
        %% It's AR Rehman
        %% Not Ryan Wizz!
        ?assertEqual({error, insufficient_tickets}, ticket_inventory:reserve(ARRahman, 101))
    end).

reserve_test() ->
    with_server(fun() ->
        {ok, HansZimmer}    = ticket_inventory:create_event("Hans Zimmer", "Royal Albert Hall", 100),
        ok = ticket_inventory:open_sale(HansZimmer),

        {ok, ReservationId} = ticket_inventory:reserve(HansZimmer, 30),
        ?assertEqual({ok, 70}, ticket_inventory:available(HansZimmer)),
        {ok, Reservation}   = ticket_inventory:reservation_info(HansZimmer, ReservationId),

        %% Ensure EventId matches
        ?assertEqual(HansZimmer, Reservation#reservation.event_id),
        ?assertEqual(30,         Reservation#reservation.quantity)
    end).

sold_out_test() ->
    with_server(fun() ->
        {ok, Ariana} = ticket_inventory:create_event("Ariana Grande", "O2", 10),
        ok = ticket_inventory:open_sale(Ariana),

        %% sell out
        {ok, _ReservationId} = ticket_inventory:reserve(Ariana, 10),
        {ok, Event}          = ticket_inventory:event_info(Ariana),
        ?assertEqual(0,        Event#event.available_tickets),
        ?assertEqual(10,       Event#event.reserved_tickets),
        ?assertEqual(sold_out, Event#event.status),

        ?assertEqual({error, insufficient_tickets}, ticket_inventory:reserve(Ariana, 1))
    end).

%% ===========================
%% ========= Release =========
%% ===========================
release_test() ->
    with_server(fun() ->
        {ok, Anoushka}      = ticket_inventory:create_event("Anoushka Shankar", "Royal Albert Hall", 100),
        ok = ticket_inventory:open_sale(Anoushka),

        {ok, ReservationId} = ticket_inventory:reserve(Anoushka, 30),
        ?assertEqual({ok, 70},                     ticket_inventory:available(Anoushka)),

        ok = ticket_inventory:release(Anoushka, ReservationId),
        ?assertEqual({ok, 100},                    ticket_inventory:available(Anoushka)),

        %% Reservation is gone now
        ?assertEqual({error, unknown_reservation}, ticket_inventory:reservation_info(Anoushka, ReservationId)),

        %% Releasing it again must fail.
        ?assertEqual({error, unknown_reservation}, ticket_inventory:release(Anoushka, ReservationId))
    end).

release_unknown_test() ->
    with_server(fun() ->
        ?assertEqual({error, unknown_event},       ticket_inventory:release(make_ref(), make_ref())),

        {ok, ARRahman} = ticket_inventory:create_event("AR Rahman", "Wembley", 100),
        ?assertEqual({error, unknown_reservation}, ticket_inventory:release(ARRahman, make_ref()))
    end).

release_sold_out_test() ->
    with_server(fun() ->
        {ok, HansZimmer}    = ticket_inventory:create_event("Hans Zimmer", "Royal Albert Hall", 10),
        ok = ticket_inventory:open_sale(HansZimmer),
        {ok, ReservationId} = ticket_inventory:reserve(HansZimmer, 10),
        ok = ticket_inventory:release(HansZimmer, ReservationId),

        %% Releasing sold
        %% out shall open
        {ok, Event}         = ticket_inventory:event_info(HansZimmer),
        ?assertEqual(on_sale, Event#event.status),
        ?assertEqual(10, Event#event.available_tickets),
        ?assertEqual(0, Event#event.reserved_tickets)
    end).
