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
%%          |       |         |      |
%%          V       V         V      V
%%        stop     stop      stop   stop
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
    [state_functions, state_enter].

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
reserved(enter, _OldState, _Data) ->
    keep_state_and_data;

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
paid(enter, _OldState, _Data) ->
    keep_state_and_data;

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
%% Dogs have been let out. And
%% we will bring `em back home
%% Terminate as terminal state
%% is reached. We lose status,
%% but it wasn't more critical
%% than removing useless pids.
%% ======== Completed ========
completed(enter, _OldState, _Data) ->
    {stop, normal}.

%% Now, bottom terminal states
%% ======== Cancelled ========
cancelled(enter, _OldState, _Data) ->
    {stop, normal}.

%% ========= Expired =========
expired(enter, _OldState, _Data) ->
    {stop, normal}.

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
