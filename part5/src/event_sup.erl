%% event_sup.erl
%%
%% Supervisor for event processes.
%% - waiting_room
%% - reservation_sup

-module(event_sup).
-moduledoc """
Each event has its own event supervisor. The event supervisor
is responsible for long-lived processes associated with event

- Waiting Room management server
- Supervisor of all reservations

Starting above processes in the one-for-one strategy.

Each event has waiting room which manages admit queue
and validates the token. Each Reservation is separate
process that receives the token and validates it. Now
other than token validation, reservation don't depend
on waiting server. Hence, we don't need rest-for-one,
that would have restarted all reservations on Waiting
room crash. Another reason is that reservations could
be in process of reserving with the ticket inventory,
Thus unnecessarily terminating active ticket booking.

There's a RACE CONDITION during waiting-room failure:
A user is admitted and receives a token, right before
waiting room crashes. The restarted waiting room will
not have the old token now. Validate for that user is
going to fail. However, this'll impact very few users
as all other users have to get new tokens. Hence this
is acceptable trade-off.
""".

-behaviour(supervisor).

%% Public APIs
-export([
    start_link/2,
    start_reservation/6
]).

%% Supervisor callback
-export([
    init/1
]).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start each Event supervisor".
-doc #{params => #{"EventId" => "Event Id for event", "AdmissionLimit" => "Max Size of Queueueue"}}.
-spec start_link(reference(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(EventId, AdmissionLimit) ->
    case supervisor:start_link(?MODULE, {EventId, AdmissionLimit}) of
        ignore -> {error, ignored};
        Result -> Result
    end.

-doc "Start a reservation for this event".
-doc #{params => #{"EventSupPid" => "Id", "EventId" => "EventId", "UserId" => "User Id", "Token" => "User Token", "Quantity" => "Num tickets", "Timeout" => "Timeout"}}.
-spec start_reservation(pid(), reference(), term(), reference(), pos_integer(), pos_integer()) -> {ok, reference()} | {error, term()}.
start_reservation(_EventSupPid, EventId, UserId, Token, Quantity, Timeout) ->
    case global:whereis_name({reservation_sup, EventId}) of
        Pid when is_pid(Pid) ->
            reservation_sup:start_reservation(Pid, EventId, UserId, Token, Quantity, Timeout);
        undefined ->
            {error, reservation_sup_not_running}
    end.

%% ============================
%% ======== Callbacks =========
%% ============================
init({EventId, AdmissionLimit}) ->
    SupFlags           = #{
        strategy  => one_for_one,
        intensity => 5,
        period    => 10
    },

     WaitingRoomSpec   = #{
        id        => waiting_room,
        start     => {waiting_room, start_link, [EventId, AdmissionLimit]},
        restart   => permanent,
        shutdown  => 5_000,
        type      => worker,
        modules   => [waiting_room]
    },

    %% Reservation supervisor manages all
    %% reservations. Reservations created
    %% dynamically by this supervisor.
    ReservationSupSpec = #{
        id        => reservation_sup,
        start     => {reservation_sup, start_link, [EventId]},
        restart   => permanent,
        shutdown  => infinity,
        type      => supervisor,
        modules   => [reservation_sup]
    },

    {ok, {SupFlags, [WaitingRoomSpec, ReservationSupSpec]}}.
