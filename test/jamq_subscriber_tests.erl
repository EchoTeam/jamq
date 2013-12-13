-module(jamq_subscriber_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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

% Tests for handle_call

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

% Tests for jamq_subscriber as a whole server

unsubscribe_immediately_test() ->
    Props = [{topic, <<"Good news, everyone!">>}, {connect_delay, 0}],
    MockStuff = mock_everything(),
    {ok, S} = jamq_subscriber:start_link(Props),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    unmock_everything(MockStuff),
    ok.

unsubscribe_after_worker_finishes_test() ->
    MockStuff = mock_everything(),
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100 -> ok end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    unmock_everything(MockStuff),
    ok.

unsubscribe_after_worker_crashes_test() ->
    MockStuff = mock_everything(),
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100 -> throw(worker_failure) end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    unmock_everything(MockStuff),
    ok.

unsubscribe_after_worker_hangs_test() ->
    MockStuff = mock_everything(),
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100500 -> ok end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    unmock_everything(MockStuff),
    ok.

example_props() ->
    [{topic, <<"Good news, everyone!">>},
     {connect_delay, 0},
     {function, fun(F) -> F() end}]. % payloads are functions of 0 arguments

send_task(Subscriber, Task) ->
    Subscriber ! {#'basic.deliver'{delivery_tag = delivery_tag, redelivered = redelivered},
                  #amqp_msg{payload = term_to_binary(Task)}}.

mock_everything() ->
    FakeChannelPid = spawn(fun() -> receive quit -> ok end end),
    meck:new(jamq_channel, [no_link]),
    meck:expect(jamq_channel, channel, 1, FakeChannelPid),
    meck:new(jamq_api, [no_link]),
    meck:expect(jamq_api, declare_queue, 6, ok),
    meck:new(lib_amqp, [no_link]),
    meck:expect(lib_amqp, ack, 2, ok),
    meck:expect(lib_amqp, subscribe, 6, ok),
    meck:expect(lib_amqp, unsubscribe, 2, ok),
    meck:expect(lib_amqp, close_channel, 1, ok),
    meck:expect(lib_amqp, bind_queue, 4, {'queue.bind_ok'}),
    meck:expect(lib_amqp, set_prefetch_count, 2, ok),
    meck:new(amqp_channel, [no_link]),
    meck:expect(amqp_channel, cast, 2, ok),
    FakeChannelPid.

unmock_everything(FakeChannelPid) ->
    FakeChannelPid ! quit,
    meck:unload().
