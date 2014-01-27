-module(jamq_tests).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {setup, fun setup/0, fun teardown/1,
         [{"Testing subscribe", fun check_subscribe/0}]}.

setup() ->
    [application:start(A) || A <- [dht_ring, compiler, syntax_tools, goldrush, lager, jamq]],
    meck:new(jamq_api),
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
