%%
%% Publisher to an AMQ.
%%
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_publisher).

-behavior(gen_server).

-ifdef(TEST).
-compile([export_all]). % used for test purpose only
-endif.

%% API
-export([start_link/1, % obsolete
         start_link/2]).

% JAMQ API
-export([async_publish/2,
         async_publish_by_key/3,
         async_transient_publish/2,
         publish/2,
         publish/3,
         publish_by_key/3,
         publish_by_key/4,
         publish_opt/2]).

-export([event_handler/2,
         format_status/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(GC_RATE, 1000).             % parrots
-define(EVENT_HANDLER_TO, 10000).   % ms


%%%===================================================================
%%% API
%%%===================================================================

% obsolete function, use start_link/2 instead
start_link(Role) ->
    start_link(Role, [Role]).

% Role - publisher's name
% Brokers - the list of brokers to connect to.
%           Separate amq channel will be established for each element of the list.
%           Also it defines message distribution between channels.
%           For example: Brokers = [b1, b2, b2]
%           it means 1/3 of all messages will be sent to b1
%           and 2/3 of all messages will be sent to b2
start_link(Role, Brokers) when is_atom(Role), is_list(Brokers) ->
    gen_server:start_link({local, name(Role)}, ?MODULE, {Role, Brokers}, []).

%%%===================================================================
%%% JAMQ API
%%%===================================================================

% NOTE: Use jamq:publish/2,3 instead!
publish(Topic, Msg) -> publish(Topic, Msg, 30000).
publish({Role, Topic}, Msg, Timeout) -> publish_ll({Role, Topic}, Msg, Timeout, undefined);
publish(Topic, Msg, Timeout) -> publish_ll({undefined, Topic}, Msg, Timeout, undefined).

% NOTE: Use jamq:publish_by_key/3,4 instead!
publish_by_key(Topic, Msg, Key) -> publish_by_key(Topic, Msg, Key, 30000).
publish_by_key({Role, Topic}, Msg, Key, Timeout) -> publish_ll({Role, Topic}, Msg, Timeout, Key);
publish_by_key(Topic, Msg, Key, Timeout) -> publish_ll({undefined, Topic}, Msg, Timeout, Key).

% NOTE: Use jamq:async_publish/2 instead!
async_publish({Role, Topic}, Msg) -> async_publish_ll({Role, Topic}, Msg, undefined);
async_publish(Topic, Msg) -> async_publish_ll({undefined, Topic}, Msg, undefined).

% NOTE: Use jamq:async_publish_by_key/3 instead!
async_publish_by_key({Role, Topic}, Msg, Key) -> async_publish_ll({Role, Topic}, Msg, Key);
async_publish_by_key(Topic, Msg, Key) -> async_publish_ll({undefined, Topic}, Msg, Key).

% Messages are NOT saved between AMQP broker restarts.
async_transient_publish({Role, Topic}, Msg) -> async_transient_publish_ll({Role, Topic}, Msg, undefined);
async_transient_publish(Topic, Msg) -> async_transient_publish_ll({undefined, Topic}, Msg, undefined).

publish_opt(Msg, Opts) when is_list(Opts) ->
    Topic = proplists:get_value(topic, Opts, ""),
    case maybe_publish(Msg) of
        true ->
            Role = proplists:get_value(broker, Opts),
            Exchange = proplists:get_value(exchange, Opts, <<"jskit-bus">>),
            DeliveryMode = case proplists:get_bool(transient, Opts) of
                    true -> 1; false -> 2 end,
            NoWait = proplists:get_bool(nowait, Opts),
            Key = proplists:get_value(key, Opts),
            gen_server:call(name(Role),
                {publish, Key, Exchange,
                    iolist_to_binary(Topic),
                    term_to_binary(wrapped_msg(Msg)),
                    DeliveryMode, NoWait});
        false -> log_message(Topic, Msg)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Role, Brokers}) ->
    {ok, RingPid} = dht_ring:start_link([{Broker, undefined, Weight} || {Broker, Weight} <- weighting(Brokers)]),
    UniqBrokers = lists:usort(Brokers),
    State = #state{
                ring = RingPid,
                role = Role,
                brokers = Brokers,
                channels = [#chan{broker = B} || B <- UniqBrokers]},
    {ok, lists:foldl(fun reconnect/2, State, UniqBrokers)}.

handle_call({publish, _Key, _Topic, _Binary} = PubMsg, From, State = #state{}) ->
    {noreply, drain_queue(State#state{queue = lists:append(State#state.queue, [{From, PubMsg}])})};

handle_call({publish, _Key, _Exchange, _Topic, _Binary, _DeliveryMode, false} = PubMsg, From, State = #state{}) ->
    {noreply, drain_queue(State#state{queue = lists:append(State#state.queue, [{From, PubMsg}])})};

handle_call({publish, _Key, _Exchange, _Topic, _Binary, _DeliveryMode, true} = PubMsg, _From, State = #state{}) ->
    {reply, ok, drain_queue(State#state{queue = lists:append(State#state.queue, [{nofrom, PubMsg}])})};

handle_call({status}, _From, #state{queue = Q} = State) ->
    {reply, [{queue, length(Q)}], State}.

handle_cast({publish, _Key, _Topic, _Binary} = PubMsg, State) ->
    {noreply, drain_queue(State#state{queue = lists:append(State#state.queue, [{nofrom, PubMsg}])})};

handle_cast({transient_publish, _Key, _Topic, _Binary} = PubMsg, State = #state{}) ->
    {noreply, drain_queue(State#state{queue = lists:append(State#state.queue, [{nofrom, PubMsg}])})}.

handle_info({'DOWN', Ref, _, _, Reason}, #state{channels = Channels} = State) ->
    {noreply, handle_down(Channels, Ref, Reason, State)};

handle_info({channel_event, Channel, #'channel.flow'{active = Flag}}, #state{channels = Channels} = State) ->
    NewChannels =
        case lists:keyfind(Channel, #chan.channel, Channels) of
            false ->
                lager:error("jamq_publisher(~p) received channel_event from unknown channel ~p", [self(), Channel]),
                Channels;
            Chan  ->
                lists:keystore(Channel, #chan.channel, Channels, Chan#chan{active = Flag})
        end,
    {noreply, drain_queue(State#state{channels = NewChannels})};

handle_info({channel_event, _Channel, #'basic.ack'{}}, State = #state{}) ->
    {noreply, drain_queue(State)};

handle_info({channel_event, _Channel, #'basic.nack'{}}, State = #state{}) ->
    {noreply, drain_queue(State)};

handle_info(acquire_channel, #state{channels = Channels} = State) ->
    lager:info("AMQ publisher ~p: acquire_channel timer expired", [self()]),
    {NewChannels, AnythingToConnect} = acquire_channels(Channels),
    NewState =
        case AnythingToConnect of
            true   -> State;
            false  ->
                catch timer:cancel(State#state.ch_timer),
                State#state{ch_timer = undefined}
        end,
    {noreply, drain_queue(NewState#state{channels = NewChannels})};

handle_info(Info, State) ->
    lager:error("jamq_publisher(~p) Unhandled info message ~10000000p", [self(), Info]),
    {noreply, State}.

terminate(Reason, #state{channels = Channels}) ->
    case Reason of
        normal -> ok;
        _      -> lager:warning("jamq_publisher(~p) terminating: ~1000000p", [self(), Reason])
    end,
    lists:foreach(
        fun (#chan{channel = Chan}) ->
            (Chan == undefined) orelse (catch lib_amqp:close_channel(Chan))
        end, Channels).

code_change(_OldVsn, State, _Extra) ->
    NewState = case State of
        {state, Role, Brokers, Channels, ChannelsTimer, Q, Counter} ->
            {ok, RingPid} = dht_ring:start_link([{Broker, undefined, Weight} || {Broker, Weight} <- weighting(Brokers)]),
            NewQ = queue:to_list(Q),
            S =  #state{ring = RingPid,
                        role = Role,
                        brokers = Brokers,
                        channels = Channels,
                        ch_timer = ChannelsTimer,
                        queue = NewQ,
                        sent_msg_count = Counter},
            lager:info("Publisher state migration: ~p", [S]),
            S;
        #state{} -> State
    end,
    {ok, NewState}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

publish_ll({Role, Topic}, Msg, Timeout, Key) ->
    case maybe_publish(Msg) of
        true  -> Binary = term_to_binary(wrapped_msg(Msg)),
                 plog:count(e2, "jamq_message_distribution_i", erlang:byte_size(Binary) div 6553),
                 gen_server:call(name(Role), {publish, Key,
                                              iolist_to_binary(Topic),
                                              Binary}, Timeout);
        false -> log_message(Topic, Msg)
    end.

async_publish_ll({Role, Topic}, Msg, Key) ->
    gen_server:cast(name(Role), {publish, Key,
                                 iolist_to_binary(Topic),
                                 term_to_binary(wrapped_msg(Msg))}).

async_transient_publish_ll({Role, Topic}, Msg, Key) ->
    gen_server:cast(name(Role), {transient_publish, Key,
                                 iolist_to_binary(Topic),
                                 term_to_binary(wrapped_msg(Msg))}).


acquire_channels(Channels) ->
    lists:mapfoldl(
        fun (Chan = #chan{channel = undefined, broker = B}, Flag) ->
            try
                ChanPid = jamq_channel:channel(B),
                set_channel_props(ChanPid, self()),
                {Chan#chan{
                    channel = ChanPid,
                    mon = erlang:monitor(process, ChanPid),
                    active = true}, Flag}
            catch
                _:_ -> {Chan, true}
            end;
            (Chan, Flag) -> {Chan, Flag}
        end,
        false, Channels).

set_channel_props(Channel, Pid) ->
    proc_lib:spawn(
        fun () ->
            % Enable ingress flow control
            amqp_channel:register_flow_handler(Channel, self()),
            amqp_channel:register_confirm_handler(Channel, self()),
            % Enable confirms
            %#'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
            erlang:link(Channel),
            event_handler(Channel, Pid)
        end),
    ok.

event_handler(Channel, Parent) ->
    receive
        Msg ->
            Parent ! {channel_event, Channel, Msg},
            ?MODULE:event_handler(Channel, Parent)
    after
        % due to code update reason
        ?EVENT_HANDLER_TO ->
            ?MODULE:event_handler(Channel, Parent)
    end.


handle_down([], Ref, Reason, #state{role = Role, channels = Channels} = State) ->
    lager:error("JAMQ Publisher ~p(~p) unhandled 'DOWN' - ~p, reason ~100000000p / channels: ~10000000p", [Role, self(), Ref, Reason, Channels]),
    State;

handle_down([#chan{publisher = {_, Ref}, msg = {From, _}} = Chan|_], Ref, normal,
            #state{channels = Channels} = State) ->
    (From == nofrom) orelse gen_server:reply(From, ok),
    NewState = State#state{channels = clean_channel(Chan, Channels)},
    drain_queue(on_msg_sent(NewState));

handle_down([#chan{publisher = {_, Ref}, msg = Msg} = Chan|_], Ref, {{error, {channel, Reason}}, _},
            #state{channels = Channels, queue = Q, role = Role} = State) ->
    lager:info("Can not publish to channel on broker ~p: ~p", [Role, Reason]),
    NewState = State#state{queue = [Msg | Q], channels = clean_channel(Chan, Channels)},
    drain_queue(NewState);

handle_down([#chan{publisher = {_, Ref}, msg = Msg, broker = B} = Chan|_], Ref, _Reason,
            #state{channels = Channels, queue = Q} = State) ->
    NewState = State#state{queue = [Msg | Q], channels = clean_channel(Chan, Channels)},
    reconnect(B, NewState);

handle_down([#chan{mon = Ref, publisher = Publisher, msg = Msg, broker = B} = Chan|_], Ref, _Reason,
            #state{channels = Channels, queue = Q} = State) ->
    NewQ = case Publisher of
        {_, PublisherMon} -> catch erlang:demonitor(PublisherMon, [flush]),
                             [Msg | Q];
        _                 -> Q
    end,
    NewState = State#state{queue = NewQ, channels = clean_channel(Chan, Channels)},
    reconnect(B, NewState);

handle_down([_ | Tail], Ref, Reason, State) ->
    handle_down(Tail, Ref, Reason, State).


clean_channel(Chan = #chan{broker = Broker}, Channels) ->
    NewChan = Chan#chan{publisher = undefined, msg = undefined},
    lists:keystore(Broker, #chan.broker, Channels, NewChan).


%% Unconditional channel reconnect
reconnect(Broker, #state{channels = Channels, ch_timer = OldTimer} = State) ->
    Channel = lists:keyfind(Broker, #chan.broker, Channels),
    catch lib_amqp:close_channel(Channel#chan.channel),
    NewTimer =
        case OldTimer == undefined of
            true ->
                {ok, TRef} = timer:send_interval(5000, acquire_channel),
                TRef;
            false ->
                OldTimer
        end,
    NewChannel = Channel#chan{channel = undefined, mon = undefined, publisher = undefined, active = false},
    State#state{
        channels = lists:keystore(Broker, #chan.broker, Channels, NewChannel),
        ch_timer = NewTimer
    }.

drain_queue(#state{queue = Q, brokers = Brokers} = State) when Q == [] orelse Brokers == [] ->
    State;
drain_queue(#state{channels = Channels, brokers = Brokers} = State) ->
    {AvailableBrokers, DownBrokers} = get_brokers(Channels, Brokers),
    drain_queue_ll(AvailableBrokers, DownBrokers, [], State#state{brokers = rotate_brokers(Brokers)}).

drain_queue_ll(AvailableBrokers, _DownBrokers, PassedMsgs, #state{queue = Q} = State) when AvailableBrokers == [] orelse Q == [] ->
    State#state{queue = PassedMsgs ++ Q};
drain_queue_ll(AvailableBrokers, DownBrokers, PassedMsgs, #state{queue = [Msg | Q], channels = Channels} = State) ->
    Broker = get_broker_candidate(AvailableBrokers, DownBrokers, State),
    {NewAvailableBrokers, NewPassedMsgs, NewChannels} = case Broker of
        []     -> {AvailableBrokers, lists:append(PassedMsgs, [Msg]), Channels};
        Broker -> {AvailableBrokers -- [Broker], PassedMsgs, send_to_channel(Broker, Msg, Channels)}
    end,
    drain_queue_ll(NewAvailableBrokers, DownBrokers, NewPassedMsgs, State#state{queue = Q, channels = NewChannels}).

send_to_channel(Broker, {_, PubMsg} = Msg, Channels) ->
    Chan = lists:keyfind(Broker, #chan.broker, Channels),
    Publisher = spawn_monitor(fun() -> amqp_publish(Chan#chan.channel, PubMsg) end),
    NewChan = Chan#chan{msg = Msg, publisher = Publisher},
    lists:keystore(Broker, #chan.broker, Channels, NewChan).

amqp_publish(Channel, {publish, _Key, Topic, Binary}) ->
    amqp_publish(Channel, <<"jskit-bus">>, Topic, Binary,
        _DeliveryMode = 2);
amqp_publish(Channel, {transient_publish, _Key, Topic, Binary}) ->
    amqp_publish(Channel, <<"jskit-bus">>, Topic, Binary,
        _DeliveryMode = 1);
amqp_publish(Channel, {publish, _Key, Exchange, Topic, Binary, DeliveryMode, _NoWait}) ->
    amqp_publish(Channel, Exchange, Topic, Binary, DeliveryMode).

amqp_publish(Channel, Exchange, Topic, Binary, DeliveryMode) ->
    Res = lib_amqp:publish(Channel, Exchange, Topic, Binary,
        #'P_basic'{
            content_type = <<"application/octet-stream">>,
            delivery_mode = DeliveryMode,
            priority = 0
        }),
    case Res of
        ok      -> ok;
        closing -> erlang:error({error, {channel, closing}});
        blocked -> erlang:error({error, {channel, throttling}});
        _       -> lager:error("Unexpected AMQ publish result: ~p", [Res]), ok
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

name(Role) when is_atom(Role) ->
    list_to_atom("jamq_publisher_" ++ atom_to_list(Role)).

weighting(Brokers) ->
    Weight = round(100/length(Brokers)),
    dict:to_list(lists:foldl(fun(Key, Dict) -> dict:update_counter(Key, Weight, Dict) end, dict:new(), Brokers)).

maybe_publish(Msg) ->
    erlang:external_size(Msg) < 50*1024*1024.

on_msg_sent(State = #state{sent_msg_count = N}) ->
    (N rem ?GC_RATE) == 0 andalso erlang:garbage_collect(),
    State#state{sent_msg_count = N + 1}.

wrapped_msg(Msg) ->
    {wrapped, [{published_timestamp, now()}], Msg}.


%% Brokers machinery

get_broker_candidate(AvailableBrokers, DownBrokers, #state{queue = [{_, Msg} | _Q], ring = RingPid}) ->
    Key = element(2, Msg),
    case Key of
        undefined -> hd(AvailableBrokers);
        Key       -> Brokers = [Broker || {Broker, _} <- dht_ring:lookup(RingPid, Key)], %% if key exists use consistent hasing
                     get_broker(Brokers, AvailableBrokers, DownBrokers)
    end.

get_broker([], _AvailableBrokers, _DownBrokers) -> [];
get_broker([Broker | Brokers], AvailableBrokers, DownBrokers) ->
    case {lists:member(Broker, AvailableBrokers), lists:member(Broker, DownBrokers)} of
        {true,  false} -> Broker;
        {false, true}  -> get_broker(Brokers, AvailableBrokers, DownBrokers);
        _              -> []
    end.

get_brokers(Channels, Brokers) ->
    {BusyBrokers, DownBrokers} = lists:foldl(
            fun (#chan{channel = undefined, broker = Broker}, {BBrokers, DBrokers}) -> {BBrokers, [Broker | DBrokers]};
                (#chan{channel = C, publisher = {_, _}, broker = Broker}, {BBrokers, DBrokers}) when is_pid(C) -> {[Broker | BBrokers], DBrokers};
                (_, Acc) -> Acc
            end, {[], []}, Channels),
    AvailableBrokers = (Brokers -- BusyBrokers) -- DownBrokers,
    {AvailableBrokers, DownBrokers}.

rotate_brokers([]) -> [];
rotate_brokers([Broker | Brokers]) -> lists:append(Brokers, [Broker]).


format_status(_Opt, [_Dict, State]) ->
    [{data, [{"State", [{role, State#state.role},
                        {queue_length, queue:len(State#state.queue)},
                        {channels, State#state.channels},
                        {sent_message_count, State#state.sent_msg_count}]}]}].

log_message(Topic, Msg) ->
    case application:get_env(jamq, large_msg_log) of
        undefined ->
            lager:error("Trying to publish large message to ~p..., the log file isn't specified", [Topic]);
        {ok, Log} ->
            lager:error("Trying to publish large message to ~p...", [Topic]),
            try
                disk_log:open([{name, log}, {file, Log}]),
                disk_log:alog(log, {Topic, Msg}),
                disk_log:close(log)
            catch C:R ->
                lager:error("Error when writing log: ~p", [{C,R}])
            end
    end.

