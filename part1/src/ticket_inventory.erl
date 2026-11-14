%% ticket_inventory.erl
%%
%% Ticket inventory management server.

-module(ticket_inventory).
-moduledoc """
Ticket inventory management server

API to create events, open events,
do reservations, and release them.
Events and reservations are stored
in ETS tables. Thus can be queried
by their IDs.

Single process to manage inventory
for all events so becomes a single
point of failure for whole system.

Another issue is that ETS table is
not persisted. It dies with owning
process. A ticket inventory crash,
loses all inventory information.

Improvements:
- Each event can maintain it's own
Inventory Server thus reducing the
impact of crash to single event.
- Aim at moving ETS to a persisted
DB service, such as postgres later

""".

-behaviour(gen_server).

-include("event.hrl").
-include("reservation.hrl").

%% Public APIs
%% Should we add delete_event API
%% that will delete the event and
%% release all its reservations?
-export([
    start_link/0,
    create_event/3,
    event_info/1,
    reservation_info/2,
    open_sale/1,
    available/1,
    reserve/2,
    release/2
]).

%% Callbacks for `gen_server`
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

-define(EVENTS_TABLE,       events).
-define(RESERVATIONS_TABLE, reservations).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start server".
-spec start_link() -> {ok, pid()} | {error, already_started}.
start_link() ->
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Pid}                        -> {ok, Pid};
        {error, {already_started, _Pid}} -> {error, already_started}  %% Override as per spec!
    end.

-doc "Create a new event".
-doc #{params => #{"Name" => "Name", "Venue" => "Venue", "TotalTickets" => "Total tickets"}}.
-spec create_event(string(), string(), pos_integer()) -> {ok, event_id()} | {error, invalid}.
create_event(Name, Venue, TotalTickets) ->
    gen_server:call(?MODULE, {create_event, Name, Venue, TotalTickets}).

-doc "Get info about an event".
-doc #{params => #{"EventId" => "Event Id for event"}}.
-spec event_info(event_id()) -> {ok, #event{}} | {error, unknown_event}.
event_info(EventId) ->
    gen_server:call(?MODULE, {event_info, EventId}).

%% Extra API as this could be useful
-doc "Get information about a reservation".
-doc #{params => #{"_EventId" => "Event Id", "ReservationId" => "Reservation Id for reservation"}}.
-spec reservation_info(event_id(), reservation_id()) -> {ok, #reservation{}} | {error, unknown_reservation}.
reservation_info(EventId, ReservationId) ->
    gen_server:call(?MODULE, {reservation_info, EventId, ReservationId}).

-doc "Open sale for given event".
-doc #{params => #{"EventId" => "Event Id for event"}}.
-spec open_sale(event_id()) -> ok | {error, unknown_event |
                                            already_open}.
open_sale(EventId) ->
    gen_server:call(?MODULE, {open_sale, EventId}).

-doc "Return num of tickets available for event".
-doc #{params => #{"EventId" => "Event Id for event"}}.
-spec available(event_id()) -> {ok, non_neg_integer()} | {error, unknown_event}.
available(EventId) ->
    gen_server:call(?MODULE, {available, EventId}).

-doc "Reserve tickets for the given event".
-doc #{params => #{"EventId" => "Event Id for event", "Quantity" => "Num tickets to reserve"}}.
-spec reserve(event_id(), pos_integer()) -> {ok, reservation_id()} | {error, unknown_event    |
                                                                             not_on_sale      |
                                                                             invalid_quantity |
                                                                             insufficient_tickets}.
reserve(EventId, Quantity) ->
    gen_server:call(?MODULE, {reserve, EventId, Quantity}).

-doc "Release reservation and free event bookings".
-doc #{params => #{"EventId" => "Event Id for event", "ReservationId" => "Reservation Id for reservation"}}.
-spec release(event_id(), reservation_id()) -> ok | {error, unknown_event |
                                                            unknown_reservation}.
release(EventId, ReservationId) ->
    gen_server:call(?MODULE, {release, EventId, ReservationId}).

%% ============================
%% ======== Callbacks =========
%% ============================
%% Initialize server with empty
%% event and reservation states
init([]) ->
    logger:info("Initialized server"),
    %% Define state as a map with events and
    %% reservations as maps of id => Object.
    %% Thus, creating this empty init state.
    ets:new(?EVENTS_TABLE,       [named_table, set, protected]), %% Let others read
    ets:new(?RESERVATIONS_TABLE, [named_table, set, protected]), %% but, not write.
    {ok, #{}}.

%% ====== Create Event =======
handle_call({create_event, [], _, _}, From, State) ->
    logger:warning("create_event: Empty name from ~p", [From]),
    {reply, {error, invalid}, State};

handle_call({create_event, _, [], _}, From, State) ->
    logger:warning("create_event: Empty venue from ~p", [From]),
    {reply, {error, invalid}, State};

handle_call({create_event, Name, Venue, TotalTickets}, From, State)
    when is_list(Name), is_list(Venue),
         is_integer(TotalTickets), TotalTickets > 0 ->
    %% Create new event
    EventId = make_ref(),
    Event = #event{
        id                = EventId,
        name              = Name,
        venue             = Venue,
        total_tickets     = TotalTickets,
        reserved_tickets  = 0,
        available_tickets = TotalTickets,
        status            = not_on_sale
    },

    %% Add it to existing Events
    %% map and then update State
    ets:insert(?EVENTS_TABLE, {EventId, Event}),

    logger:info("create_event: Created ~p at ~p with ~p tickets from ~p", [Name, Venue, TotalTickets, From]),
    {reply, {ok, EventId}, State};

handle_call({create_event, _, _, _}, From, State) ->
    logger:warning("create_event: Invalid request from ~p", [From]),
    {reply, {error, invalid}, State};

%% ========= Get Info =========
%% Return information about an existing event
%% Match Event by the EventId and return that
handle_call({event_info, EventId}, _From, State) ->
    {reply, lookup_event(EventId), State};

%% Return information about an existing reservation
handle_call({reservation_info, EventId, ReservationId}, _From, State) ->
    case lookup_reservation(ReservationId) of
        {ok, Reservation} when Reservation#reservation.event_id =:= EventId ->
            {reply, {ok, Reservation}, State};
        _ ->
            {reply, {error, unknown_reservation}, State}
    end;

%% ======== Open Sale ========
%% Opens ticket sales for an existing event
handle_call({open_sale, EventId}, From, State) ->
    case lookup_event(EventId) of
        {ok, Event} ->
            handle_open_sale(EventId, From, State, Event);
        {error, unknown_event} ->
            {reply, {error, unknown_event}, State}
    end;

%% ==== Available Tickets ====
%% Doesn't matter if it's open
handle_call({available, EventId}, _From, State) ->
    case lookup_event(EventId) of
        {ok, Event} ->
            {reply, {ok, Event#event.available_tickets}, State};
        {error, unknown_event} ->
            {reply, {error, unknown_event}, State}
    end;

%% ========= Reserve ========
handle_call({reserve, EventId, Quantity}, From, State) ->
    case lookup_event(EventId) of
        {ok, Event} ->
            handle_reserve(EventId, Quantity, From, State, Event);
        {error, unknown_event} ->
            {reply, {error, unknown_event}, State}
    end;

%% ======== Release =========
handle_call({release, EventId, ReservationId}, From, State) ->
    case lookup_event(EventId) of
        {error, unknown_event} ->
            {reply, {error, unknown_event}, State};
        {ok, Event} ->
            case lookup_reservation(ReservationId) of
                {error, unknown_reservation} ->
                    {reply, {error, unknown_reservation}, State};
                {ok, Reservation} ->
                    handle_release(EventId, ReservationId, From, State, Event, Reservation)
            end
    end;

%% Ignore unmatched
handle_call(_, _From, State) ->
    {reply, {error, unknown_api}, State}.

%% Satisfy Sir General Server
handle_cast(_, State) ->
    {noreply, State}.

%% ===========================
%% ======== Internal =========
%% ===========================
lookup_event(EventId) ->
    case ets:lookup(?EVENTS_TABLE, EventId) of
        [{EventId, Event}] ->
            {ok, Event};
        [] ->
            {error, unknown_event}
    end.

lookup_reservation(ReservationId) ->
    case ets:lookup(?RESERVATIONS_TABLE, ReservationId) of
        [{ReservationId, Reservation}] ->
            {ok, Reservation};
        [] ->
            {error, unknown_reservation}
    end.

%% ======= Open Sale =========
handle_open_sale(EventId, From, State, Event = #event{status = not_on_sale}) ->
    UpdatedEvent = Event#event{status = on_sale},
    ets:insert(?EVENTS_TABLE, {EventId, UpdatedEvent}),

    logger:info("open_sale: Opened sale for event ~p from ~p", [EventId, From]),
    {reply, ok, State};

handle_open_sale(EventId, From, State, #event{status = Status}) ->
    %% Capture both open and sold out
    %% as sold out is already opened!
    logger:info("open_sale: Invalid request for ~p event ~p from ~p", [Status, EventId, From]),
    {reply, {error, already_open}, State}.

%% ========= Reserve ========
%% Error if event not on sale
handle_reserve(_EventId, _Quantity, _From, State, #event{status = not_on_sale}) ->
    {reply, {error, not_on_sale}, State};

%% Reserve tickets against an
%% event with new reservation
%% and allocating from event.
handle_reserve(EventId, Quantity, From, State,
               Event = #event{available_tickets = AvailableTickets,
                              reserved_tickets  = ReservedTickets})
    when is_integer(Quantity), Quantity > 0, AvailableTickets >= Quantity ->
    ReservationId = make_ref(),

    Reservation   = #reservation{
        id        = ReservationId,
        event_id  = EventId,
        quantity  = Quantity
    },

    ets:insert(?RESERVATIONS_TABLE, {ReservationId, Reservation}),

    %% Update existing Event
    %% ensuring total ticket
    %% invariant maintained!
    NewAvailable = AvailableTickets - Quantity,
    NewReserved  = ReservedTickets  + Quantity,
    NewStatus    = case NewAvailable of
                        0 -> sold_out;
                        _ -> on_sale
                    end,

    UpdatedEvent = Event#event{
        available_tickets = NewAvailable,
        reserved_tickets  = NewReserved,
        status            = NewStatus
    },

    ets:insert(?EVENTS_TABLE, {EventId, UpdatedEvent}),

    logger:info("reserve: Reserved [~p] ~p tickets for ~p from ~p",[ReservationId, Quantity, EventId, From]),
    {reply, {ok, ReservationId}, State};

handle_reserve(_EventId, Quantity, _From, State, _Event)
    when not is_integer(Quantity); Quantity =< 0 ->
    %% event exists but the quantity is invalid?
    {reply, {error, invalid_quantity}, State};

handle_reserve(_EventId, _Quantity, _From, State, _Event) ->
    %% This should capture event exists but is either
    %% sold out or requested quantity can't be filled
    {reply, {error, insufficient_tickets}, State}.

%% ======== Release =========
%% Release tickets against an
%% event deleting reservation
%% and thus free up the event
handle_release(EventId, ReservationId, From, State,
               Event        = #event{available_tickets = AvailableTickets,
                                     reserved_tickets  = ReservedTickets},
               _Reservation = #reservation{event_id    = EventId,
                                           quantity    = Quantity}) ->
    %% invariant maintained!
    NewAvailable = AvailableTickets + Quantity,
    NewReserved  = ReservedTickets  - Quantity,

    %% New Status is updated to on_sale,
    %% if it was sold_out. But if it was
    %% not_on_sale (for future improvs),
    %% then keep that same status intact
    NewStatus    = case Event#event.status of
                        sold_out -> on_sale;
                        Status   -> Status
                   end,

    UpdatedEvent = Event#event{
        available_tickets = NewAvailable,
        reserved_tickets  = NewReserved,
        status            = NewStatus
    },

    ets:insert(?EVENTS_TABLE,       {EventId, UpdatedEvent}),
    ets:delete(?RESERVATIONS_TABLE, ReservationId),

    logger:info("release: Released [~p] ~p tickets for ~p from ~p", [ReservationId, Quantity, EventId, From]),
    {reply, ok, State};

handle_release(_EventId, _ReservationId, _From, State, _Event, _Reservation) ->
    {reply, {error, unknown_reservation}, State}.
