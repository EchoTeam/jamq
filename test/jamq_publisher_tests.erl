-module(jamq_publisher_tests).

-record(chan, {
    broker    = undefined :: atom(),
    channel   = undefined :: pid(),
    mon       = undefined :: reference(),
    publisher = undefined :: {pid(), reference()},
    msg       = undefined :: {From :: gen_server:from(), Msg :: term()},
    active    = false     :: true | false
}).

-record(state, {
    ring           = undefined   :: pid(),               % DHT Ring for brokers
    role           = undefined   :: atom(),              % Queue role for precise publishing
    brokers        = []          :: [nonempty_string()], % Defines order to send messages
    channels       = []          :: [#chan{}],           % Active AMQP Channels
    ch_timer       = undefined   :: timer:tref(),        % Reconnection timer
    queue          = []          :: list(),              % Excess queue
    sent_msg_count = 0           :: pos_integer()
}).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
  {setup, fun setup/0, fun cleanup/1,
     [{"Get broker", fun get_broker_/0},
	  {"drain_queue_ll round robin", fun drain_queue_ll_round_robin_/0},
	  {"drain_queue_ll dht ring", fun drain_queue_ll_dht_ring_/0},
	  {"drain_queue", fun drain_queue_/0}]}.

get_broker_() ->
    ?assertEqual(broker1, jamq_publisher:get_broker([broker1, broker2, broker3], [broker1, broker2, broker3], [])), % broker1 available
    ?assertEqual([], jamq_publisher:get_broker([broker1, broker2, broker3], [broker2, broker3], [])), % broker1 busy

    ?assertEqual(broker2, jamq_publisher:get_broker([broker1, broker2, broker3], [broker2, broker3], [broker1])), % broker1 down, broker2 available
    ?assertEqual([], jamq_publisher:get_broker([broker1, broker2, broker3], [broker3], [broker1])), % broker2 busy

    ?assertEqual(broker3, jamq_publisher:get_broker([broker1, broker2, broker3], [broker3], [broker1, broker2])), % broker1 down, broker2 down, broker3 available
    ?assertEqual([], jamq_publisher:get_broker([broker1, broker2, broker3], [], [broker1, broker2])). % broker3 busy

drain_queue_ll_round_robin_() ->
    Q0 = [{client1, {publish, undefined, undefined, msg1}},
          {client2, {publish, undefined, undefined, msg2}},
          {client2, {publish, undefined, undefined, msg3}},
          {client3, {publish, undefined, undefined, msg4}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q1 = [{client3, {publish, undefined, undefined, msg4}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q2 = [{client2, {publish, undefined, undefined, msg3}},
          {client3, {publish, undefined, undefined, msg4}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q3 = [{client3, {publish, undefined, undefined, msg6}}],

    Q4 = [{client1, {publish, undefined, undefined, msg1}},
          {client2, {publish, undefined, undefined, msg2}}],

    Q5 = [{client1, {publish, undefined, undefined, msg1}},
          {client2, {publish, undefined, undefined, msg2}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q6 = [{client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Channels = [#chan{broker = broker1, active = true},
                #chan{broker = broker2, active = true},
                #chan{broker = broker3, active = true}],

    % all ok
    ?assertMatch(#state{queue = Q1}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q0, channels = Channels})),
    ?assertMatch(#state{queue = []}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q1, channels = Channels})),

    % broker3 down
    ?assertMatch(#state{queue = Q2}, jamq_publisher:drain_queue_ll([broker1, broker2], [], [], #state{queue = Q0, channels = Channels})),
    ?assertMatch(#state{queue = Q3}, jamq_publisher:drain_queue_ll([broker1, broker2], [broker3], [], #state{queue = Q1, channels = Channels})),

    % broker3 down, broker2 busy, Q4 messages undelivered
    ?assertMatch(#state{queue = Q5}, jamq_publisher:drain_queue_ll([broker1], [broker3], Q4, #state{queue = Q1, channels = Channels})),
    ?assertMatch(#state{queue = Q6}, jamq_publisher:drain_queue_ll([broker1, broker2], [broker3], [], #state{queue = Q5, channels = Channels})),
    ok.

drain_queue_ll_dht_ring_() ->
    Q0 = [{client1, {publish, keyA, undefined, msg1}},
          {client2, {publish, keyA, undefined, msg2}},
          {client2, {publish, keyB, undefined, msg3}},
          {client3, {publish, keyA, undefined, msg4}},
          {client1, {publish, keyB, undefined, msg5}},
          {client3, {publish, keyA, undefined, msg6}}],

    Q1 = [{client2, {publish, keyA, undefined, msg2}},
          {client3, {publish, keyA, undefined, msg4}},
          {client1, {publish, keyB, undefined, msg5}},
          {client3, {publish, keyA, undefined, msg6}}],

    Q2 = [{client3, {publish, keyA, undefined, msg4}},
          {client3, {publish, keyA, undefined, msg6}}],

    Q3 = [{client3, {publish, keyA, undefined, msg6}}],

    Q4 = [{client2, {publish, keyA, undefined, msg2}},
          {client3, {publish, keyA, undefined, msg4}},
          {client3, {publish, keyA, undefined, msg6}}],

    Q5 = [{client1, {publish, keyB, undefined, msg5}}],

    Q6 = [{client1, {publish, keyB, undefined, msg5}},
          {client3, {publish, keyA, undefined, msg4}},
          {client3, {publish, keyA, undefined, msg6}}],

    Channels = [#chan{broker = broker1, active = true},
                #chan{broker = broker2, active = true},
                #chan{broker = broker3, active = true}],

    % all ok
    ?assertMatch(#state{queue = Q1}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q0, channels = Channels})),
    ?assertMatch(#state{queue = Q2}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q1, channels = Channels})),
    ?assertMatch(#state{queue = Q3}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q2, channels = Channels})),
    ?assertMatch(#state{queue = []}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q3, channels = Channels})),

    % broker1 busy
    ?assertMatch(#state{queue = Q1}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q0, channels = Channels})),
    ?assertMatch(#state{queue = Q4}, jamq_publisher:drain_queue_ll([broker2, broker3], [], [], #state{queue = Q1, channels = Channels})),

    % broker1 down, Q5 messages undelivered
    ?assertMatch(#state{queue = Q6}, jamq_publisher:drain_queue_ll([broker2, broker3], [broker1], Q5, #state{queue = Q4, channels = Channels})),
    ?assertMatch(#state{queue = Q2}, jamq_publisher:drain_queue_ll([broker2, broker3], [broker1], [], #state{queue = Q6, channels = Channels})),

    % broker1 down, broker2 down
    ?assertMatch(#state{queue = Q3}, jamq_publisher:drain_queue_ll([broker3], [broker1, broker2], [], #state{queue = Q2, channels = Channels})),
    % broker1 busy, broker2 down
    ?assertMatch(#state{queue = Q3}, jamq_publisher:drain_queue_ll([broker3], [broker2], [], #state{queue = Q3, channels = Channels})),
    ?assertMatch(#state{queue = []}, jamq_publisher:drain_queue_ll([broker1, broker2, broker3], [], [], #state{queue = Q3, channels = Channels})),
    ok.

drain_queue_() ->
    Q0 = [{client1, {publish, undefined, undefined, msg1}},
          {client2, {publish, undefined, undefined, msg2}},
          {client2, {publish, undefined, undefined, msg3}},
          {client3, {publish, undefined, undefined, msg4}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q1 = [{client3, {publish, undefined, undefined, msg4}},
          {client1, {publish, undefined, undefined, msg5}},
          {client3, {publish, undefined, undefined, msg6}}],

    Q2 = [{client3, {publish, undefined, undefined, msg6}}],

    Channels0 = [#chan{channel = list_to_pid("<0.1.0>"), publisher = undefined, broker = broker1, active = true},
                 #chan{channel = list_to_pid("<0.2.0>"), publisher = undefined, broker = broker2, active = true},
                 #chan{channel = list_to_pid("<0.3.0>"), publisher = undefined, broker = broker3, active = true}],

    Channels1 = [#chan{channel = list_to_pid("<0.1.0>"), publisher = undefined, broker = broker1, active = true},
                 #chan{channel = list_to_pid("<0.2.0>"), publisher = {pid,ref}, broker = broker2, msg = {client2, {publish, undefined, undefined, msg2}}, active = true},
                 #chan{channel = list_to_pid("<0.3.0>"), publisher = undefined, broker = broker3, active = true}],

    Channels2 = [#chan{channel = undefined, publisher = undefined, broker = broker1, active = true},
                 #chan{channel = list_to_pid("<0.2.0>"), publisher = undefined, broker = broker2, active = true},
                 #chan{channel = list_to_pid("<0.3.0>"), publisher = undefined, broker = broker3, active = true}],

    % all ok
    ?assertMatch(#state{queue = Q1,
                        channels = [#chan{broker = broker1, msg = {client1, {publish, undefined, undefined, msg1}}, active = true},
                                    #chan{broker = broker2, msg = {client2, {publish, undefined, undefined, msg2}}, active = true},
                                    #chan{broker = broker3, msg = {client2, {publish, undefined, undefined, msg3}}, active = true}],
                        brokers = [broker2, broker3, broker1]},
                 jamq_publisher:drain_queue(#state{queue = Q0, channels = Channels0, brokers = [broker1, broker2, broker3]})),

    % broker2 busy
    ?assertMatch(#state{queue = Q2,
                        channels = [#chan{broker = broker1, msg = {client1, {publish, undefined, undefined, msg5}}, active = true},
                                    #chan{broker = broker2, msg = {client2, {publish, undefined, undefined, msg2}}, active = true},
                                    #chan{broker = broker3, msg = {client3, {publish, undefined, undefined, msg4}}, active = true}],
                        brokers = [broker3, broker1, broker2]},
                 jamq_publisher:drain_queue(#state{queue = Q1, channels = Channels1, brokers = [broker2, broker3, broker1]})),

    % broker1 down
    ?assertMatch(#state{queue = Q2,
                        channels = [#chan{broker = broker1, msg = undefined, active = true},
                                    #chan{broker = broker2, msg = {client3, {publish, undefined, undefined, msg4}}, active = true},
                                    #chan{broker = broker3, msg = {client1, {publish, undefined, undefined, msg5}}, active = true}],
                        brokers = [broker3, broker1, broker2]},
                 jamq_publisher:drain_queue(#state{queue = Q1, channels = Channels2, brokers = [broker2, broker3, broker1]})),

    ok.

setup() ->
    meck:new(dht_ring),
    meck:expect(dht_ring, lookup, fun(_, keyA) -> [{broker1, 25}, {broker2, 25}, {broker3, 25}];
                                     (_, keyB) -> [{broker2, 25}, {broker1, 25}, {broker3, 25}];
                                     (_, _)    -> [{broker3, 25}, {broker2, 25}, {broker1, 25}] end),
    meck:new(jamq_api),
    meck:expect(jamq_api, publish, fun(_, _, _, _, _) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

