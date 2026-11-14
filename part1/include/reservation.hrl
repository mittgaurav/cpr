%% reservation.hrl
%%
%% Every reservation is associated
%% with an event via the unique id.
%% The reservation has unique id as
%% well. Also, a reservation can be
%% including multiple quantities.

-ifndef(RESERVATION_HRL).
-define(RESERVATION_HRL, true).

-include("event.hrl").

%% Unique reservaton id
-type reservation_id() :: reference().

%% Reservation record
-record(reservation, {
    id       :: reservation_id(),
    event_id :: event_id(),    %% Foreign key
    quantity :: pos_integer()  %% One reservation can have multiple seats.
}).

-endif.
