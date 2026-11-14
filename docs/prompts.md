# Reservation Terminal States
In the below erlang code, the supposed terminal states are still hanging around because I want to return the status of the state of reservation:

```erlang
%% reservation.erl
%%
%% Reservation finite state machine.
%%
%% start -> reserved -- pay --> paid
%%          | \   / |           /  \
%%          | fail  |          /    \
%%        cancel timeout   cancel complete
%%          |       |         |      |
%%          V       V         V      V
%%     cancelled expired  cancelled completed
%%
-module(reservation).

-moduledoc """
Each reservation is its own gen_statem
The process validates admission token,
reserves tickets from ticket inventory
server to manage reservation lifecycle
""".

%% Implementing FSMs in python
%% or C is much more intuitive
%% than with this behaviour :(
-behaviour(gen_statem).  %% succeeds gen_fsm

-type state() :: reserved | paid | completed | cancelled | expired.

%% Public APIs
-export([
    start_link/5,
    %% Have a start_link
    %% for supervisor.
    start_link/6,
    status/1,
    pay/2,
    complete/1,
    cancel/1
]).

%% State callbacks
-export([
    init/1,
    callback_mode/0,
    reserved/3,
    paid/3,
    completed/3,
    cancelled/3,
    expired/3
]).

%% ===========================
%% ======= Client APIs =======
%% ===========================
%% catch if process doesn't exist
%% in order to fulfill spec error
call_reservation(ReservationRef, Request, Error) ->
    try gen_statem:call({global, {?MODULE, ReservationRef}}, Request)
    catch exit:{noproc, _} -> {error, Error} end.

-doc "Start FSM".
-doc #{params => #{"EventId" => "EventId", "UserId" => "UserId", "Token" => "User Token", "Quantity" => "Num Tickets", "Timeout" => "Reservation Timeout"}}.
-spec start_link(reference(), term(), reference(), pos_integer(), pos_integer()) -> {ok, reference()} | {error, term()}.
start_link(EventId, UserId, Token, Quantity, Timeout) ->
    %% Make reference
    ReservationRef = make_ref(),
    case start_link(EventId, ReservationRef, UserId, Token, Quantity, Timeout) of
        {ok, _Pid}      -> {ok, ReservationRef};
        {error, Reason} -> {error, Reason}
    end.

%% ====== YA Start Link =======
%% Supervisor expects a pid and
%% not a reference, so use this
%% `start_link` from supervisor
%% that returns pid. Supervisor
%% must create Ref on its side.
start_link(EventId, ReservationRef, UserId, Token, Quantity, Timeout) ->
   gen_statem:start_link({global, {?MODULE, ReservationRef}}, ?MODULE,
                               {EventId, ReservationRef, UserId, Token, Quantity, Timeout}, []).


-doc "Return current reservation state".
-spec status(reference()) -> {ok, state()} | {error, unknown_reservation}.
status(ReservationRef) ->
    call_reservation(ReservationRef, status, unknown_reservation).

-doc "Attempt to pay for a reservation".
-spec pay(reference(), term()) -> ok | {error, invalid_state | payment_failed}.
%% Unknown reservation shall perhaps
%% return unknown_reservation error.
pay(ReservationRef, PaymentDetails) ->
    call_reservation(ReservationRef, {pay, PaymentDetails}, invalid_state).

-doc "Complete a paid reservation".
-spec complete(reference()) -> {ok, [reference()]} | {error, invalid_state}.
complete(ReservationRef) ->
    call_reservation(ReservationRef, complete, invalid_state).

-doc "Cancel a reservation - reserved or paid".
-spec cancel(reference()) -> ok | {error, invalid_state | unknown_reservation}.
cancel(ReservationRef) ->
    call_reservation(ReservationRef, cancel, unknown_reservation).

%% ============================
%% ======== Callbacks =========
%% ============================
callback_mode() ->
    state_functions.

%% ========== Init ============
init({EventId, ReservationRef, UserId, Token, Quantity, Timeout}) ->
    %% First validate admission token
    case waiting_room:validate(EventId, Token) of
        ok ->
            %% Token is valid, attempt to
            %% reserve requested quantity
            case ticket_inventory:reserve(EventId, Quantity) of
                {ok, ReservationId} ->
                    Data = #{
                        reservation_ref => ReservationRef,
                        event_id        => EventId,
                        user_id         => UserId,
                        token           => Token,
                        quantity        => Quantity,
                        reservation_id  => ReservationId,
                        timeout         => Timeout
                    },

                    logger:info("reservation: Created ~p for event ~p, user ~p", [ReservationRef, EventId, UserId]),

                    %% Start in the reserved
                    %% Apparently gen_statem
                    %% gives timeout options
                    %% timeout iff users are
                    %% in reserved foreverrr
                    %% {state_timeout, Time, EventContent}
                    {ok, reserved, Data, [{state_timeout, Timeout, expired}]};
                {error, Reason} ->
                    {stop, {reservation_failed, Reason}}
            end;
        {error, Reason} ->
            {stop, {invalid_token, Reason}}
    end.

%% ============================
%% ========= States ===========
%% ============================
%% {next_state, NextState, NewData, Actions}
%% {next_state, NextState, NewData}
%% {keep_state,            NewData, Actions}
%% {keep_state_and_data,            Actions}
%% ======== Reserved ==========
%% Status check
reserved({call, From}, status, _Data) ->
    {keep_state_and_data, [{reply, From, {ok, reserved}}]};

%% Payment attempt
reserved({call, From}, {pay, PaymentDetails}, Data) ->
    case process_payment(PaymentDetails) of
        ok ->
            %% Success - move to Paid
            logger:info("reservation: ~p payment successful from ~p", [maps:get(reservation_ref, Data), From]),

            %% I am assuming timeout cancels
            %% on state change from reserved
            {next_state, paid, Data, [{reply, From, ok}]};
        error ->
            %% Failure - Stay Reserved
            logger:warning("reservation: ~p payment failed from ~p", [maps:get(reservation_ref, Data), From]),

            %% Stay in reserved
            {keep_state_and_data, [{reply, From, {error, payment_failed}}]}
    end;

%% Cancel - Release tickets
reserved({call, From}, cancel, Data) ->
    release_tickets(Data),

    logger:info("reservation: ~p cancelled", [maps:get(reservation_ref, Data)]),
    {next_state, cancelled, Data, [{reply, From, ok}]};

%% Timeout - Release tickets
reserved(state_timeout, expired, Data) ->
    release_tickets(Data),

    logger:info("reservation: ~p expired", [maps:get(reservation_ref, Data)]),
    {next_state, expired, Data};

%% Unkown
reserved({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, invalid_state}}]}.

%% ========== Paid ===========
%% NO Time-outs
%% Status check
paid({call, From}, status, _Data) ->
    {keep_state_and_data, [{reply, From, {ok, paid}}]};

%% Payment again is not allowed.
paid({call, From}, {pay, _PaymentDetails}, _Data) ->
    {keep_state_and_data,
     [{reply, From, {error, invalid_state}}]};

%% Complete - issue tickets
paid({call, From}, complete, Data) ->
    %% Generate tickets
    TicketRefs = create_ticket_refs(maps:get(quantity, Data)),

    logger:info("reservation: ~p completed", [maps:get(reservation_ref, Data)]),
    %% Update ticket in State
    %% for future references.
    %% E.g. Release cancelled
    {next_state, completed, Data#{ticket_refs => TicketRefs}, [{reply, From, {ok, TicketRefs}}]};

%% Cancel after payment
paid({call, From}, cancel, Data) ->
    %% Release tickets
    release_tickets(Data),

    logger:info("reservation: ~p cancelled after payment", [maps:get(reservation_ref, Data)]),
    {next_state, cancelled, Data, [{reply, From, ok}]};

%% Unknown
%% When idiots like me
%% try payment again!!
paid({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, invalid_state}}]}.

%% ===== Terminal States =====
%% Dogs have been let out. But
%% who will bring them home???
%% Terminating process without
%% terminal states messes up -
%% status is lost. Ideally I'd
%% terminate but keeping these
%% to reply the status rightly
%% ======== Completed ========
completed({call, From}, status, _Data) ->
    {keep_state_and_data, [{reply, From, {ok, completed}}]};

completed({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, invalid_state}}]}.

%% Now, bottom terminal states
%% ======== Cancelled ========
cancelled({call, From}, status, _Data) ->
    {keep_state_and_data, [{reply, From, {ok, cancelled}}]};

cancelled({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, invalid_state}}]}.

%% ========= Expired =========
expired({call, From}, status, _Data) ->
    {keep_state_and_data, [{reply, From, {ok, expired}}]};

expired({call, From}, _Request, _Data) ->
    {keep_state_and_data, [{reply, From, {error, invalid_state}}]}.

%% ===========================
%% ======== Internal =========
%% ===========================
%% Placeholder check - payment
%% details have to be verified
process_payment(#{success := true}) ->
    %% just check that success field
    %% is present and is set to true
    ok;

process_payment(_) ->
    error.

%% release ticket inventory
release_tickets(#{event_id := EventId, reservation_id := ReservationId}) ->
    %% logs success, do i
    %% wanna log failure?
    ticket_inventory:release(EventId, ReservationId).

%% Create ticket references
create_ticket_refs(Quantity) ->
    create_ticket_refs(Quantity, []).

create_ticket_refs(0, Acc) ->
    Acc;

create_ticket_refs(Quantity, Acc) ->
    create_ticket_refs(Quantity - 1, [make_ref() | Acc]).
```

........ I checked with professor the requirement:
> In Part 3, it says that the FSM must include completed state, and the flowchart shows that paid transitions to completed on complete/1.
>
> However, Part 3 also states that calling complete/1 after payment should terminate the process. These two requirements seem rather contradictory. Am I missing something?

........ and their response was:
> No, it is not contradictory. You are in the state completed, which is the terminal state. That is where you send the stop message and terminate the process. Just like you do with cancelled and expired. You would do different things in completed, cancelled and expired when you receive the stop message.

......... can you please explain what the professor meant and what am i still missing in this?
The original requirement in assignment is:

```text
Implementing the Reservation Process
Once a user has been admitted from the waiting room, they may attempt to reserve tickets
for the event. A reservation should be modelled as a finite state machine, representing the
lifecycle of a user's attempt to buy tickets. A reservation may be created, paid for, completed,
cancelled, or allowed to expire if the user does not complete the purchase within the allotted
time. When a reservation expires or is cancelled, the tickets must be released back into the
ticket inventory.
The reservation process should be implemented in the reservation.erl module and
should export the following functions:
start
_
link(EventId, UserId, Token, Quantity, Timeout) -> {ok, ReservationRef} |
{error, Reason}.
status(ReservationRef) -> {ok, Status} | {error, unknown
_
reservation}.
pay(ReservationRef, PaymentDetails) -> ok | {error, invalid
_
state | payment
_
failed}.
complete(ReservationRef) -> {ok, TicketRefs} | {error, invalid
_
state}.
cancel(ReservationRef) -> ok | {error, invalid
_
state | unknown
_
reservation}.
EventId is the event identifier created by the ticket inventory server. UserId is the user
attempting to buy tickets. Token is the admission token issued by the waiting room.
Quantity is the number of tickets requested, and Timeout is the number of milliseconds
for which the reservation remains valid. ReservationRef is a unique opaque reference
generated by the system.
The reservation finite state machine should include at least the following states: reserved,
paid, completed, cancelled and expired. When start_link/5 is called, the process
should validate the admission token and attempt to reserve the requested number of tickets
from the ticket inventory server. If successful, the reservation enters the reserved state. If
the timeout expires before payment is completed, the reservation enters the expired state
and releases the tickets. Calling cancel/1 before completion should also release the
tickets. Calling complete/1 after successful payment should issue ticket references and
terminate the reservation process normally.
```

### Follow up
but this is getting rid of the states, i want to keep the states an then perhaps do the action in them somehow. is that possible?

# Load Test
here is the content of my implementation: %% ticket_load.erl
```erlang
%%
%% Simple distributed load test
%% for the ticketing system.
-module(ticket_load).

-export([run/4]).
-export([
    lookup_event_sup/1,
    get_event_id/1,
    first_event/0
]).

-define(TIMEOUT, 5000).
-define(BATCH, 10).

%% Start the load test.
run(TicketNode, EventId, Users, MaxTickets) ->
    case net_adm:ping(TicketNode) of
        pong ->
            %% connected
            load(TicketNode, EventId, Users, MaxTickets);
        pang ->
            {error, cannot_connect}
    end.

load(TicketNode, EventId, Users, MaxTickets) ->
    % elp:ignore W0014 (cross_node_eval)
    case rpc:call(TicketNode, ticket_load, lookup_event_sup, [EventId]) of
        {ok, EventSup} ->
            Parent = self(),

            [spawn(fun() -> user(Parent, TicketNode, EventId, EventSup, Id, MaxTickets)
            end) || Id <- lists:seq(1, Users)],

            timer:sleep(100),
            admit(TicketNode, EventId, Users),

            Results = collect(Users, []),
            {ok, count(Results)};
        Error ->
            Error
    end.

%% One simulated user.
user(Parent, TicketNode, EventId, EventSup, Id, MaxTickets) ->
    UserId = {load_user, node(), erlang:unique_integer([positive]), Id},

    % elp:ignore W0014 (cross_node_eval)
    Result = rpc:call(TicketNode, waiting_room, join, [EventId, UserId, self()]),

    case Result of
        {ok, queued, _Position} ->
            wait_for_admission(Parent, TicketNode, EventId, EventSup, UserId, MaxTickets);

        Other ->
            Parent ! {result, failed, Other}
    end.

%% Wait until the waiting room admits the user.
wait_for_admission(Parent, TicketNode, EventId, EventSup, UserId, MaxTickets) ->
    receive
        {admitted, EventId, Token} ->
            Quantity = rand:uniform(MaxTickets),

            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, event_sup, start_reservation, [EventSup, EventId, UserId, Token, Quantity, ?TIMEOUT]),
            reservation(Parent, TicketNode, Result)
    after 15_000 ->
        Parent ! {result, queued}
    end.

%% Decide what the user does with the reservation.
reservation(Parent, TicketNode, {ok, Ref}) ->
    case rand:uniform(100) of
        N when N =< 60 ->
            % elp:ignore W0014 (cross_node_eval)
            rpc:call(TicketNode, reservation, pay, [Ref, #{success => true}]),

            % elp:ignore W0014 (cross_node_eval)
            Result = rpc:call(TicketNode, reservation, complete, [Ref]),
            Parent ! {result, completed, Result};

        N when N =< 85 ->
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

reservation(Parent, _TicketNode, {error, insufficient_tickets}) ->
    Parent ! {result, failed, insufficient_tickets};

reservation(Parent, _TicketNode, Error) ->
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
collect(0, Results) ->
    Results;

collect(N, Results) ->
    receive
        {result, Result} ->
            collect(N - 1, [Result | Results]);

        {result, Type, Result} ->
            collect(N - 1, [{Type, Result} | Results])
    after 30_000 ->
        Results
    end.

%% Count the important results.
count(Results) ->
    Completed = count_result(completed, Results),
    Cancelled = count_result(cancelled, Results),
    Expired   = count_result(expired, Results),
    Queued    = count_result(queued, Results),
    Failed    = count_result(failed, Results),

    #{
        completed => Completed,
        cancelled => Cancelled,
        expired   => Expired,
        queued    => Queued,
        failed    => Failed,
        results   => Results
    }.


count_result(Type, Results) ->
    length([
        ok
        || {ResultType, _} <- Results,
           ResultType =:= Type
    ]).

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
```
........ Here is the assignment details of the part:
```text
Implementing the Distributed Load
Generator
To test the behaviour of the ticketing system under load, implement a load generator which
runs in a separate distributed Erlang node from the ticketing system. The load generator
should simulate a large number of users attempting to buy tickets for the same event at
approximately the same time. Each simulated user should join the waiting room, wait to be
admitted, attempt to reserve tickets, and then either complete, cancel or abandon the
reservation.
The load generator should be implemented in the ticket_load.erl module and should
export the following functions:
run(TicketNode, EventId, UserCount, MaxTicketsPerUser) -> {ok, Summary} |
{error, Reason}.
run(TicketNode, EventId, UserCount, MaxTicketsPerUser, Options) -> {ok, Summary} |
{error, Reason}.
TicketNode is the name of the Erlang node on which the ticketing system is running. The
load generator should use rpc:call/4 or rpc:call/5 to call the public API of the
ticketing system from the load generator node. EventId is the event being tested.
UserCount is the number of simulated users attempting to access the system.
MaxTicketsPerUser is the maximum number of tickets any one simulated user may
request. Options may be used to configure the behaviour of the test, including the
percentage of users who complete payment, cancel their reservation or abandon it until
timeout.
The load generator should report a summary of the run, including the number of users
admitted, queued, successfully ticketed, rejected because insufficient tickets were available,
cancelled, expired and failed because of errors. It should also report the number of tickets
sold, reserved and remaining at the end of the test. Your implementation should include trial
runs which show the system handling a traffic spike where the number of users is greater
than the number of tickets available.
You should include instructions showing how to start the ticketing node (as a distributed
node) and the load generator node, how to connect them using distributed Erlang, and how
to run the load test. Your report should discuss whether the bottleneck is in the waiting room,
ticket inventory server, reservation processes, remote procedure calls, or elsewhere in your
design.
```
......... can you make following improvement, keeping the code, comments, and spacing exact same unless required to be changed:
1. add missing run/5 that takes in options for keys payment_rate, cancel_rate, abandon_rate. run/4 should call it, with defaults of let's say 60, 25, 15. pass down to each user process so hardcoded uniform in reseration/3 use these values.
2. summary map must also include admitted, insufficient_tickets count and a final snapshot of tickets sold, tickets_reserved, tickets_remaining, from rpc:call to ticket_inventory:event_info()

### Follow up
```text
(load@127.0.0.1)11> ticket_load:run('ticket@127.0.0.1', EventId, 1000, 1).
Command is taking a long time, type Ctrl+G, then enter 'i' to interrupt
{ok,#{cancelled => 32,failed => 849,expired => 23,
      results =>
          [{expired,{error,unknown_reservation}},
           {expired,{error,unknown_reservation}},
           {expired,{error,unknown_reservation}},
           {expired,{error,unknown_reservation}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,insufficient_tickets}}},
           {failed,{error,{reservation_failed,...}}},
           {failed,{error,{...}}},
           {failed,{error,...}},
           {failed,{...}},
           {failed,...},
           {...}|...],
      insufficient_tickets => 0,queued => 0,admitted => 1000,
      completed => 96,tickets_sold => 0,tickets_reserved => 0,
      tickets_remaining => 0}}
(load@127.0.0.1)12>
```
..... this doesn't look right - it shows all in admitted while the error gives insufficent tiket. what is this

# Ticket Full Boot
Given these files Here is my Erlang/OTP ticketing system – ticket_load.erl, ticket_sup.erl, and ticket_inventory.erl.

```erlang
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
        {ok, {event, EventId, _Name, _Venue, _TotalTickets, TicketsSold, TicketsRemaining, _Status}}
        when is_number(TicketsSold), is_number(TicketsRemaining) ->
            #{
                admitted             => Admitted,
                completed            => Completed,
                cancelled            => Cancelled,
                expired              => Expired,
                queued               => Queued,
                insufficient_tickets => InsufficientTickets,
                failed               => Failed,
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
```
......>>
```erlang
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
```
.........
```erlang
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
-doc #{params => #{"_EventId" => "Event Id unused", "ReservationId" => "Reservation Id for reservation"}}.
-spec reservation_info(event_id(), reservation_id()) -> {ok, #reservation{}} | {error, unknown_reservation}.
reservation_info(_EventId, ReservationId) ->
    gen_server:call(?MODULE,{reservation_info, ReservationId}).

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
handle_call({reservation_info, ReservationId}, _From, State) ->
    {reply, lookup_reservation(ReservationId), State};

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
```

Create a ticket_boot.erl helper module with three exports:

1. compiles all source modules in src/ with [{i, "include"}] using a wrapper.
   Modules: ticket_inventory, waiting_room, reservation, reservation_sup, event_sup, event_sup_sup,
   ticket_sup, ticket_load. Print each module name as it compiles.

2. start – compiles then ticket_sup:start_link(), creates 3 events using
   ticket_sup:start_event/4, opens sales with ticket_inventory:open_sale/1, prints each event.
   Events:
   - "Sonu Nigam" at "Wembley", 100 tickets, admission limit 20
   - "Disney" at "Royal Albert", 50 tickets, admission limit 10
   - "Arijit Singh" at "O2 Arena", 200 tickets, admission limit 50

3. demo – takes TicketNode atom, compile, pings the ticket node, fetches all events from the
   event_sup_registry ETS table via rpc:call, spawns ticket_load:runs on each event in parallel (2x the
   ticket count as users, max 5 tickets each), collects results, prints per-event summary and final ticket
   state via ticket_inventory:event_info. Phew

Usage: one c("src/ticket_boot", [{i, "include"}]) per shell, then ticket_boot:start() on the ticket node,
ticket_boot:demo('ticket@127.0.0.1') on the load node.

Keep the same coding style as the above files.. short comments, ignore error W0014 on rpc:call, no libraries outside OTP et al.
