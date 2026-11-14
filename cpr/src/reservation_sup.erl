%% reservation_sup.erl
%%
%% Dynamic supervisor for reservation processes belonging to
%% one event.
%%

-module(reservation_sup).
-moduledoc """
Each reservation is an independent process. A reservation
finishing normally, being cancelled, or expiring must not
cause any other reservation to restart.

Reservations are therefore temporary and dynamic children
""".

-behaviour(supervisor).

%% Public API
-export([
    start_link/1,
    start_reservation/6
]).

-export([
    init/1
]).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start each reservation supervisor".
-doc #{params => #{"EventId" => "Event Id"}}.
-spec start_link(reference()) ->  {ok, pid()} | {error, term()}.
start_link(EventId) ->
    case supervisor:start_link({global, {?MODULE, EventId}}, ?MODULE, [EventId]) of
        ignore -> {error, ignored};
        Result -> Result
    end.

-doc "Start reservation child".
-doc #{params => #{"SupPid" => "Id", "EventId" => "EventId", "UserId" => "UserId", "Token" => "Token", "Quantity" => "Max Tickets", "Timeout" => "Timeout"}}.
-spec start_reservation(pid(), reference(), term(), reference(), pos_integer(), pos_integer()) -> {ok, reference()} | {error, term()}.
start_reservation(SupPid, _EventId, UserId, Token, Quantity, Timeout) ->
    %% This! was weird and puzzling
    %% but I suppose makes sense in
    %% retrospect. supervisor child
    %% must return pid. Reservation
    %% instead returned Ref.
    ReservationRef = make_ref(),
    case supervisor:start_child(SupPid, [ReservationRef, UserId, Token, Quantity, Timeout]) of
        {ok, _Pid} when is_reference(ReservationRef) ->
            {ok, ReservationRef};
        {error, Reason} ->
            {error, Reason}
    end.

%% ============================
%% ======== Callbacks =========
%% ============================
init([EventId]) ->
    SupFlags        = #{
        strategy  => simple_one_for_one,
        intensity => 10,
        period    => 10
    },

    %% If it dies, it dies
    ReservationSpec = #{
        id        => reservation,
        start     => {reservation, start_link, [EventId]},
        restart   => temporary,
        shutdown  => 5_000,
        type      => worker,
        modules   => [reservation]
    },

    {ok, {SupFlags, [ReservationSpec]}}.
