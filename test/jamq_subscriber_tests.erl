-module(jamq_subscriber_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

% Tests for jamq_subscriber as a whole server
run_test_() ->
  {setup, fun setup/0, fun cleanup/1,
     [{"Unsubscribe immediately", fun unsubscribe_immediately_/0},
	  {"Unsubscribe after worker finishes", fun unsubscribe_after_worker_finishes_/0},
	  {"Unsubscribe after worker crashes", fun unsubscribe_after_worker_crashes_/0},
	  {"Unsubscribe after worker hangs", {timeout, 25, [fun unsubscribe_after_worker_hangs_/0]}},
	  {"Pass invalid functions", fun pass_invalid_functions_/0}]}.

unsubscribe_immediately_() ->
    {ok, S} = jamq_subscriber:start_link(example_props()),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    ok.

unsubscribe_after_worker_finishes_() ->
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100 -> ok end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    ok.

unsubscribe_after_worker_crashes_() ->
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100 -> throw(worker_failure) end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    ok.

unsubscribe_after_worker_hangs_() ->
    {ok, S} = jamq_subscriber:start_link(example_props()),
    send_task(S, fun () ->
                     receive after 100500 -> ok end
                 end),
    {ok, {unsubscribed, <<"Good news, everyone!">>}} = jamq_subscriber:unsubscribe(S),
    ok.

pass_invalid_functions_() ->
    Fun = fun() -> ok end,
    {stop, {invalid_function, {Fun, _}}} = jamq_subscriber:init([{topic, <<"Good news, everyone!">>}, {function, Fun}]),
    {stop, {invalid_function, {undefined, _}}} = jamq_subscriber:init([{topic, <<"Good news, everyone!">>}]),
    ok.

payload(F) -> F().

example_props() ->
    [{topic, <<"Good news, everyone!">>},
     {connect_delay, 0},
     {function, fun ?MODULE:payload/1}].

send_task(Subscriber, Task) ->
    Subscriber ! {#'basic.deliver'{delivery_tag = delivery_tag, redelivered = redelivered},
                  #amqp_msg{payload = term_to_binary(Task)}}.

setup() ->
    FakeChannelPid = spawn(fun() -> receive quit -> ok end end),
    meck:new(jamq_channel),
    meck:expect(jamq_channel, channel, 1, FakeChannelPid),
    meck:new(jamq_api),
    meck:expect(jamq_api, declare_queue, 6, ok),
    meck:expect(jamq_api, ack, 2, ok),
    meck:expect(jamq_api, subscribe, 6, ok),
    meck:expect(jamq_api, unsubscribe, 2, ok),
    meck:expect(jamq_api, close_channel, 1, ok),
    meck:expect(jamq_api, bind_queue, 4, {'queue.bind_ok'}),
    meck:expect(jamq_api, set_prefetch_count, 2, ok),
    meck:new(amqp_channel),
    meck:expect(amqp_channel, cast, 2, ok),
    FakeChannelPid.

cleanup(FakeChannelPid) ->
    FakeChannelPid ! quit,
    meck:unload().
