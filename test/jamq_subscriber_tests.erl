-module(jamq_subscriber_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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
    meck:new(jamq_channel),
    meck:expect(jamq_channel, channel, 1, FakeChannelPid),
    meck:new(jamq_api),
    meck:expect(jamq_api, declare_queue, 6, ok),
    meck:new(lib_amqp),
    meck:expect(lib_amqp, ack, 2, ok),
    meck:expect(lib_amqp, subscribe, 6, ok),
    meck:expect(lib_amqp, unsubscribe, 2, ok),
    meck:expect(lib_amqp, close_channel, 1, ok),
    meck:expect(lib_amqp, bind_queue, 4, {'queue.bind_ok'}),
    meck:expect(lib_amqp, set_prefetch_count, 2, ok),
    meck:new(amqp_channel),
    meck:expect(amqp_channel, cast, 2, ok),
    FakeChannelPid.

unmock_everything(FakeChannelPid) ->
    FakeChannelPid ! quit,
    meck:unload().
