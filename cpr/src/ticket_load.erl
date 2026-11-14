%% ticket_load.erl
%%
%% Simple distributed load test
%% for the ticketing system.
-module(ticket_load).

-export([run/4]).
-export([run/5]).
-export([
    lookup_event_sup/1,
    get_event_id/1,
    first_event/0
]).

-define(TIMEOUT, 5000).
-define(BATCH, 10).

%% Start the load test.
run(TicketNode, EventId, Users, MaxTickets) ->
    run(TicketNode, EventId, Users, MaxTickets, #{
        payment_rate => 60,
        cancel_rate => 25,
        abandon_rate => 15
    }).

run(TicketNode, EventId, Users, MaxTickets, Options) ->
    case net_adm:ping(TicketNode) of
        pong ->
            %% connected
            load(TicketNode, EventId, Users, MaxTickets, Options);
        pang ->
            {error, cannot_connect}
    end.

load(TicketNode, EventId, Users, MaxTickets, Options) ->
    % elp:ignore W0014 (cross_node_eval)
    case rpc:call(TicketNode, ticket_load, lookup_event_sup, [EventId]) of
        {ok, EventSup} ->
            Parent = self(),

            [spawn(fun() -> user(Parent, TicketNode, EventId, EventSup, Id, MaxTickets, Options)
            end) || Id <- lists:seq(1, Users)],

            timer:sleep(100),
            admit(TicketNode, EventId, Users),

            {Results, Admitted} = collect(Users, [], 0),
            {ok, count(Results, Admitted, TicketNode, EventId)};
        Error ->
            Error
    end.

%% One simulated user.
user(Parent, TicketNode, EventId, EventSup, Id, MaxTickets, Options) ->
    UserId = {load_user, node(), erlang:unique_integer([positive]), Id},

    % elp:ignore W0014 (cross_node_eval)
    Result = rpc:call(TicketNode, waiting_room, join, [EventId, UserId, self()]),

    case Result of
        {ok, queued, _Position} ->
            wait_for_admission(Parent, TicketNode, EventId, EventSup, UserId, MaxTickets, Options);

        Other ->
            Parent ! {result, failed, Other}
    end.

%% Wait until the waiting room admits the user.
wait_for_admission(Parent, TicketNode, EventId, EventSup, UserId, MaxTickets, Options) ->
    receive
        {admitted, EventId, Token} ->
            Parent ! admitted,
            Quantity = rand:uniform(MaxTickets),

            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, event_sup, start_reservation, [EventSup, EventId, UserId, Token, Quantity, ?TIMEOUT]),
            reservation(Parent, TicketNode, Result, Options)
    after 15_000 ->
        Parent ! {result, queued}
    end.

%% Decide what the user does with the reservation.
reservation(Parent, TicketNode, {ok, Ref}, Options) ->
    PaymentRate = maps:get(payment_rate, Options),
    CancelRate = maps:get(cancel_rate, Options),

    case rand:uniform(100) of
        N when N =< PaymentRate ->
            % elp:ignore W0014 (cross_node_eval)
            rpc:call(TicketNode, reservation, pay, [Ref, #{success => true}]),

            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, reservation, complete, [Ref]),
            Parent ! {result, completed, Result};

        N when N =< PaymentRate + CancelRate ->
            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, reservation, cancel, [Ref]),
            Parent ! {result, cancelled, Result};

        _ ->
            %% Do nothing. Reservation will expire.
            timer:sleep(?TIMEOUT + 100),

            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, reservation, status, [Ref]),
            Parent ! {result, expired, Result}
    end;

reservation(Parent, _TicketNode, {error, {reservation_failed, insufficient_tickets}}, _Options) ->
    Parent ! {result, insufficient_tickets, insufficient_tickets};

reservation(Parent, _TicketNode, Error, _Options) ->
    Parent ! {result, failed, Error}.

%% Admit users in batches.
admit(TicketNode, EventId, Users) ->
    admit(TicketNode, EventId, Users, ?BATCH).

admit(_TicketNode, _EventId, 0, _Batch) ->
    ok;

admit(TicketNode, EventId, Users, Batch) ->
    N = min(Users, Batch),

    % elp:ignore W0014 (cross_node_eval)
    rpc:call(TicketNode, waiting_room, admit, [EventId, N]),

    timer:sleep(100),
    admit(TicketNode, EventId, Users - N, Batch).

%% Collect results from all simulated users.
collect(0, Results, Admitted) ->
    {Results, Admitted};

collect(N, Results, Admitted) ->
    receive
        admitted ->
            collect(N, Results, Admitted + 1);

        {result, Result} ->
            collect(N - 1, [Result | Results], Admitted);

        {result, Type, Result} ->
            collect(N - 1, [{Type, Result} | Results], Admitted)
    after 30_000 ->
        {Results, Admitted}
    end.

%% Count the important results.
count(Results, Admitted, TicketNode, EventId) ->
    Completed           = count_result(completed, Results),
    Cancelled           = count_result(cancelled, Results),
    Expired             = count_result(expired, Results),
    Queued              = count_result(queued, Results),
    Failed              = count_result(failed, Results),
    InsufficientTickets = count_result(insufficient_tickets, Results),

    % elp:ignore W0014 (cross_node_eval)
    Snapshot = rpc:call(TicketNode, ticket_inventory, event_info, [EventId]),

    case Snapshot of
        {ok, {event, EventId, _Name, _Venue, TotalTickets, TicketsSold, TicketsRemaining, _Status}}
        when is_number(TicketsSold), is_number(TicketsRemaining) ->
            #{
                admitted             => Admitted,
                completed            => Completed,
                cancelled            => Cancelled,
                expired              => Expired,
                queued               => Queued,
                insufficient_tickets => InsufficientTickets,
                failed               => Failed,
                tickets_total        => TotalTickets,
                tickets_sold         => TicketsSold,
                tickets_reserved     => TicketsSold - Completed,
                tickets_remaining    => TicketsRemaining,
                results              => Results
            };
        Error ->
            #{
                admitted             => Admitted,
                completed            => Completed,
                cancelled            => Cancelled,
                expired              => Expired,
                queued               => Queued,
                insufficient_tickets => InsufficientTickets,
                failed               => Failed,
                tickets_total        => 0,
                tickets_sold         => 0,
                tickets_reserved     => 0,
                tickets_remaining    => 0,
                results              => Results,
                inventory_error      => Error
            }
    end.

count_result(Type, Results) ->
    length([ ok || {ResultType, _} <- Results, ResultType =:= Type]).

%% Runs on the ticket node.
lookup_event_sup(EventId) ->
    case ets:lookup(event_sup_registry, EventId) of
        [{EventId, Pid}] when is_pid(Pid) ->
            {ok, Pid};
        [] ->
            {error, not_found}
    end.

%% Get the first event ID from the ticket node.
get_event_id(TicketNode) ->
    % elp:ignore W0014 (cross_node_eval)
    rpc:call(TicketNode, ticket_load, first_event, []).

%% Runs on the ticket node.
first_event() ->
    case ets:tab2list(event_sup_registry) of
        [{EventId, _Pid} | _] ->
            {ok, EventId};
        [] ->
            {error, no_events}
    end.
