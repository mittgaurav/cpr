%% event_sup_sup.erl
%%
%% Supervisor for all event supervisors.
%%
%% - Each event gets its own event_sup process.
%% - Event supervisors are dynamically started.

-module(event_sup_sup).
-moduledoc """
Supervisor for dynamic event supervisors

Each event is independent. Hence, an
error or crash in one doesn't impact
other event supervisors.

Simple-one-for-one as all events are
same configuration, with only passed
arguments changing.
""".

-behaviour(supervisor).

%% Public APIs
-export([
    start_link/0,
    start_event_sup/2,
    stop_event_sup/1
]).

%% Supervisor callback
-export([
    init/1
]).

%% Event supervisor pid
%% indexed by event id.
%% Useful during lookup
%% when stopping events
-define(EVENT_SUPS, event_sup_registry).

%% ===========================
%% ======= Client APIs =======
%% ===========================
-doc "Start event supervisor supervisor".
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        ignore -> {error, ignored};
        Result -> Result
    end.

-doc "Start new event supervisor".
-doc #{params => #{"EventId" => "EventId", "AdmissionLimit" => "Max Size of Queue for Waiting Room"}}.
-spec start_event_sup(reference(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_event_sup(EventId, AdmissionLimit) ->
    %% Argument checks throw inside
    case supervisor:start_child(?MODULE, [EventId, AdmissionLimit]) of
        {ok, Pid} when is_pid(Pid)->
            %% Store event supervisor pid
            ets:insert(?EVENT_SUPS, {EventId, Pid}),
            {ok, Pid};
        {ok, undefined} ->
            {error, undefined_child};
        {error, Reason} ->
            {error, Reason}
    end.

-doc "Stop event supervisor".
-spec stop_event_sup(reference()) -> ok | {error, not_found | term()}.
stop_event_sup(EventId) ->
    %% Lookup supervisor pid by event id
    case ets:lookup(?EVENT_SUPS, EventId) of
        [{EventId, Pid}] ->
            case supervisor:terminate_child(?MODULE, Pid) of
                ok ->
                    %% Remove event from ETS map
                    ets:delete(?EVENT_SUPS, EventId),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        [] ->
            {error, not_found}
    end.

%% ============================
%% ======== Callbacks =========
%% ============================
init([]) ->
    %% Track event supervisor
    %% processes in ETS table
    ets:new(?EVENT_SUPS, [named_table, set, public]),

    SupervisorFlags = #{
        strategy  => simple_one_for_one,
        intensity => 10,
        period    => 5
    },

    %% if terminated normally, we
    %% may want to stop an event.
    EventSup        = #{
        id        => event_sup,
        start     => {event_sup, start_link, []},
        restart   => transient,
        shutdown  => 5_000,
        type      => supervisor,
        modules   => [event_sup]
    },

    {ok, {SupervisorFlags, [EventSup]}}.
