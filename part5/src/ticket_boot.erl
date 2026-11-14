%% ticket_boot.erl
%%
%% Helper for compiling, starting, and
%% demonstrating the ticketing system.

-module(ticket_boot).

-export([
    compile/0,
    start/0,
    demo/1
]).

-define(SRC, "src/").
-define(INCLUDE, "include").

%% ===========================
%% ======= Compile API =======
%% ===========================

compile() ->
    Modules = [
        ticket_inventory,
        waiting_room,
        reservation,
        reservation_sup,
        event_sup,
        event_sup_sup,
        ticket_sup,
        ticket_load
    ],

    [compile_module(Module) || Module <- Modules],
    ok.

compile_module(Module) ->
    io:format("Compiling ~p~n", [Module]),
    File = filename:join(?SRC, atom_to_list(Module) ++ ".erl"),
    compile:file(File, [{i, ?INCLUDE}]).

%% ===========================
%% ======== Start API ========
%% ===========================

start() ->
    compile(),

    case ticket_sup:start_link() of
        {ok, _Pid} ->
            start_events();
        {error, {already_started, _Pid}} ->
            start_events();
        Error ->
            io:format("Could not start ticket system: ~p~n", [Error]),
            Error
    end.

start_events() ->
    Events = [
        {"Sonu Nigam", "Wembley", 100, 20},
        {"Disney", "Royal Albert", 50, 10},
        {"Arijit Singh", "O2 Arena", 200, 50}
    ],

    [start_event(Name, Venue, Tickets, AdmissionLimit) ||
        {Name, Venue, Tickets, AdmissionLimit} <- Events],

    ok.

start_event(Name, Venue, Tickets, AdmissionLimit) ->
    case ticket_sup:start_event(Name, Venue, Tickets, AdmissionLimit) of
        {ok, EventId} ->
            case ticket_inventory:open_sale(EventId) of
                ok ->
                    io:format(
                        "Started event ~p at ~p: ~p~n",
                        [Name, Venue, EventId]
                    ),
                    {ok, EventId};
                Error ->
                    io:format(
                        "Could not open sale for ~p: ~p~n",
                        [Name, Error]
                    ),
                    Error
            end;

        Error ->
            io:format(
                "Could not start event ~p: ~p~n",
                [Name, Error]
            ),
            Error
    end.

%% ===========================
%% ======== Demo API =========
%% ===========================

demo(TicketNode) ->
    compile(),

    case net_adm:ping(TicketNode) of
        pong ->
            demo_events(TicketNode);
        pang ->
            {error, cannot_connect}
    end.

demo_events(TicketNode) ->
    % elp:ignore W0014 (cross_node_eval)
    case rpc:call(TicketNode, ets, tab2list, [event_sup_registry]) of
        Events when is_list(Events) ->
            run_demo(TicketNode, Events);
        Error ->
            Error
    end.

run_demo(TicketNode, Events) ->
    Parent = self(),

    Workers = [
        spawn(fun() ->
            demo_event(Parent, TicketNode, EventId, Tickets)
        end)
        || {EventId, _Pid} <- Events,
           {ok, {_event, _Id, _Name, _Venue, Tickets, _Reserved,
                 _Available, _Status}} <-
               [get_event_info(TicketNode, EventId)]
    ],

    collect_demo(TicketNode, length(Workers), []).

demo_event(Parent, TicketNode, EventId, Tickets) ->
    Users = Tickets * 2,

    % elp:ignore W0014 (cross_node_eval)
    Result = rpc:call(
        TicketNode,
        ticket_load,
        run,
        [TicketNode, EventId, Users, 5]
    ),

    Parent ! {demo_result, EventId, Result}.

%% ===========================
%% ======== Collect ==========
%% ===========================

collect_demo(_TicketNode, 0, _Results) ->
    ok;

collect_demo(TicketNode, N, Results) ->
    receive
        {demo_result, EventId, Result} ->
            print_summary(EventId, Result),
            print_final_state(TicketNode, EventId),
            collect_demo(
                TicketNode,
                N - 1,
                [{EventId, Result} | Results]
            )
    after 120_000 ->
        io:format("Timed out waiting for demo results~n"),
        print_summaries(Results),
        ok
    end.

%% ===========================
%% ======== Helpers ==========
%% ===========================

get_event_info(TicketNode, EventId) ->
    % elp:ignore W0014 (cross_node_eval)
    rpc:call(
        TicketNode,
        ticket_inventory,
        event_info,
        [EventId]
    ).

print_summaries([]) ->
    ok;

print_summaries([{EventId, Result} | Results]) ->
    print_summary(EventId, Result),
    print_summaries(Results).

print_summary(EventId, {ok, Summary}) ->
    io:format(
        "Event ~p summary:~n"
        "  admitted:            ~p~n"
        "  completed:           ~p~n"
        "  cancelled:           ~p~n"
        "  expired:             ~p~n"
        "  queued:              ~p~n"
        "  insufficient:        ~p~n"
        "  failed:              ~p~n"
        "  tickets total:       ~p~n"
        "  tickets sold:        ~p~n"
        "  tickets reserved:    ~p~n"
        "  tickets remaining:   ~p~n",
        [
            EventId,
            maps:get(admitted,             Summary, 0),
            maps:get(completed,            Summary, 0),
            maps:get(cancelled,            Summary, 0),
            maps:get(expired,              Summary, 0),
            maps:get(queued,               Summary, 0),
            maps:get(insufficient_tickets, Summary, 0),
            maps:get(failed,               Summary, 0),
            maps:get(tickets_total,        Summary, 0),
            maps:get(tickets_sold,         Summary, 0),
            maps:get(tickets_reserved,     Summary, 0),
            maps:get(tickets_remaining,    Summary, 0)
        ]
    );

print_summary(EventId, Error) ->
    io:format(
        "Event ~p load test failed: ~p~n",
        [EventId, Error]
    ).

print_final_state(TicketNode, EventId) ->
    % elp:ignore W0014 (cross_node_eval)
    case rpc:call(
        TicketNode,
        ticket_inventory,
        event_info,
        [EventId]
    ) of
        {ok, Event} ->
            io:format(
                "Final ticket state ~p: ~p~n",
                [EventId, Event]
            );
        Error ->
            io:format(
                "Could not get final state for ~p: ~p~n",
                [EventId, Error]
            )
    end.