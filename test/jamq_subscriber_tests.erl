-module(jamq_subscriber_tests).

-include_lib("eunit/include/eunit.hrl").

-record(subscription, {
    qname = node(),
    exchange = undefined,
    qdurable = false,
    qexclusive = true, % queue.declare exclusive (queue will be deleted after client disconnection)
    exclusive = false, % basic.consume exclusive
    qauto_delete = true,
    qbind_tag = <<$#>>,
    topic = <<$#>>,
    broker = undefined,
    channel_properties = [],
    auto_ack = false,
    queue_args = [],
    status_callback = undefined,
    redelivery_ind = false
    }).

-record(state, {
    channel = undefined,
    function,
    messages = queue:new(),
    messages_retry_timer,
    message_processor,    % Process which hangles messages.
    ch_monref,
    ch_timer,
    subscription = #subscription{topic = <<"Test Topic">>},
    recv_msg_count = 0,
    supress_error = false,
    processes_waiting_for_unsubscribe = []
    }).

unsubscribe_when_idle_test() ->
    BeforeState = #state{message_processor = undefined},
    AfterState = BeforeState,
    {stop, normal, {ok, {unsubscribed, <<"Test Topic">>}}, AfterState} =
        jamq_subscriber:handle_call({unsubscribe}, {self(), test}, BeforeState).

unsubscribe_when_working_test() ->
    BeforeState = #state{message_processor = loop_forever},
    From = {self(), test},
    AfterState = BeforeState#state{processes_waiting_for_unsubscribe = [From]},
    {noreply, AfterState} =
        jamq_subscriber:handle_call({unsubscribe}, {self(), test}, BeforeState).

multiple_unsubscribers_test() ->
    BeforeState = #state{message_processor = loop_forever, processes_waiting_for_unsubscribe = [foo, bar, baz]},
    From = {self(), test},
    AfterState = BeforeState#state{processes_waiting_for_unsubscribe = [From, foo, bar, baz]},
    {noreply, AfterState} =
        jamq_subscriber:handle_call({unsubscribe}, {self(), test}, BeforeState).

% TODO: Figure out how to test other parts.
%       The problem is that they depend on timer, gen_server
%       and impure internal functions, which can't be mocked.
