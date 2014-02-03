-module(jamq_tests).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {setup, fun setup/0, fun teardown/1,
         [{"Testing subscribe", fun check_subscribe/0},
          {"lazy init", {timeout, 30, [fun lazy_init/0]}}]}.

setup() ->
    [application:start(A) || A <- [dht_ring, compiler, syntax_tools, goldrush, lager, jamq]],
    meck:new(jamq_api),
    meck:expect(jamq_api, start_connection, fun (_) ->  spawn(fun () -> timer:sleep(100000) end) end),
    meck:expect(jamq_api, close_connection, fun (_) -> ok end),
    meck:expect(jamq_api, declare_permanent_exchange, fun (_, _, _) -> ok end),
    meck:expect(jamq_api, close_channel, fun (_) -> ok end),
    meck:expect(jamq_api, publish, fun(_, _, _, _, _) -> ok end),
    meck:expect(jamq_api, subscribe, fun(_, _, _, _, _, _) -> ok end).

teardown(_) ->
    meck:unload().

handler(_) -> ok.

check_subscribe() ->
    {ok, ServerRef} = jamq:subscribe([{queue, "hello"},
                                      {topic, "#"},
                                      {broker, "broker"},
                                      {queue_bind_tag, "#"},
                                      {function, fun ?MODULE:handler/1}]),
    jamq:unsubscribe(ServerRef).

lazy_init() ->
    Self = self(),
    meck:expect(jamq_api, start_channel, fun (C) -> Self ! {start_channel, C}, spawn(fun () -> timer:sleep(100000) end) end),
    meck:expect(dht_ring, start_link, fun (_) -> {ok, Self} end),
    {ok, C} = jamq_channel:start_link(test_connection, "B1"),
    {ok, _P} = jamq_publisher:start_link(role, [test_connection]),
    receive
        {start_channel, _} -> ?assert(get_channel_received)
    after 1000 -> ok
    end,
    jamq_publisher:publish({role, "testQ"}, <<"test">>),
    receive
        {start_channel, _} -> ok
    after 4000 -> ?assert(no_get_channel_msg)
    end,
    jamq_publisher:stop(role, normal),
    jamq_channel:stop(C, normal).


