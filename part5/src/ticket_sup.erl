%% ticket_sup.erl
%%
%% Ticketing System supervisor
%%
%% Starts
%% - One ticket inventory server
%% - One supervisor for event supervisors

-module(ticket_sup).
-moduledoc """
Starting the shared services,
in the rest-for-one strategy.

Single ticket Inventory server and
supervisor for events supervisors.

If ticketing server goes down, the
entire system becomes incosistent.
Given ETS is stored in the process
memory, inventory server going down
wipes out all event and reservation
data. All event references are lost

Hence, restart the entire system if
ticket inventory process goes down.
""".

-behaviour(supervisor).

-include("event.hrl").

%% Public APIs
-export([
    start_link/0,
    start_event/4,
    stop_event/1
]).

%% Supervisor callbacks
-export([
    init/1
]).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start ticketing system supervisor".
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        %% handle ignore return to
        %% match `start_link` spec
        ignore -> {error, ignored};
        Result -> Result
    end.

-doc "Start a new event".
-doc #{params => #{"Name" => "Name", "Venue" => "Venue", "TotalTickets" => "Total tickets", "AdmissionLimit" => "Max Size of Queue"}}.
-spec start_event(string(), string(), pos_integer(), pos_integer()) -> {ok, event_id()} | {error, term()}.
start_event(Name, Venue, TotalTickets, AdmissionLimit) ->
    %% First create event with
    %% Ticket inventory server
    case ticket_inventory:create_event(Name, Venue, TotalTickets) of
        {ok, EventId} ->
            %% Then start event supervisor
            %% under supervisor-supervisor
            case event_sup_sup:start_event_sup(EventId, AdmissionLimit) of
                {ok, _Pid} ->
                        {ok, EventId};
                {error, Reason} ->
                    %% It leaves event hanging
                    %% in the inventory server
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-doc "Stop an event".
-spec stop_event(event_id()) -> ok | {error, term()}.
stop_event(EventId) ->
    event_sup_sup:stop_event_sup(EventId).

%% ============================
%% ======== Callbacks =========
%% ============================
init([]) ->
    SupervisorFlags  = #{
        strategy  => rest_for_one,
        intensity => 10,
        period    => 5
    },

    TicketInventory = #{
        id        => ticket_inventory,
        start     => {ticket_inventory, start_link, []},
        restart   => permanent,
        shutdown  => 5_000,
        type      => worker
    },

    EventSupSup     = #{
        id        => event_sup_sup,
        start     => {event_sup_sup, start_link, []},
        restart   => permanent,
        shutdown  => 5_000,
        type      => supervisor
    },


    {ok, {SupervisorFlags, [TicketInventory, EventSupSup]}}.
