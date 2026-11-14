%% event.hrl
%%
%% Every event has a unique event id.
%% Sale status is be one of not sale,
%% on sale, or sold out. Total num of
%% tickets & available tickets stored.

-ifndef(EVENT_HRL).
-define(EVENT_HRL, true).

%% Unique event id
-type event_id()      :: reference().

%% Acceptable sale status
-type status()   :: not_on_sale | on_sale | sold_out.

%% Event record
-record(event, {
    id                :: event_id(),
    name              :: string(),
    venue             :: string(),
    total_tickets     :: pos_integer(),
    reserved_tickets  :: non_neg_integer(),  %% can be zero
    available_tickets :: non_neg_integer(),
    status            :: status()
}).

-endif.
