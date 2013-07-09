%%
%% Publisher to an AMQ.
%%
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_publisher).
-behavior(gen_server).
-export([
    async_publish/2,
    async_transient_publish/2,
    publish/2,
    publish/3,
    publish_opt/2,
    start_link/1, % obsolete
    start_link/2,
    event_handler/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/2
]).

-include_lib("rabbitmq/include/rabbit.hrl").
-include_lib("rabbitmq/include/rabbit_framing.hrl").

name(Role) when is_atom(Role) ->
    list_to_atom("jamq_publisher_" ++ atom_to_list(Role)).

publish_opt(Msg, Opts) when is_list(Opts) ->
    Role = proplists:get_value(broker, Opts),
    Exchange = proplists:get_value(exchange, Opts, <<"jskit-bus">>),
    Topic = proplists:get_value(topic, Opts, ""),
    DeliveryMode = case proplists:get_bool(transient, Opts) of
            true -> 1; false -> 2 end,
    NoWait = proplists:get_bool(nowait, Opts),
    gen_server:call(name(Role),
        {publish, Exchange,
            iolist_to_binary(Topic),
            term_to_binary(wrapped_msg(Msg)),
            DeliveryMode, NoWait}).

% NOTE: Use jamq:publish/2,3 instead!
publish(Topic, Msg) ->
    publish(Topic, Msg, 30000).
publish(Topic, Msg, Timeout) when is_list(Topic) ->
    publish({undefined, Topic}, Msg, Timeout);
publish({Role, Topic}, Msg, Timeout) ->
    gen_server:call(name(Role), {publish,
        iolist_to_binary(Topic),
        term_to_binary(wrapped_msg(Msg))}, Timeout).

% NOTE: Use jamq:async_publish/2 instead!
async_publish(Topic, Msg) when is_list(Topic) ->
    async_publish({undefined, Topic}, Msg);
async_publish({Role, Topic}, Msg) ->
    gen_server:cast(name(Role), {publish,
        iolist_to_binary(Topic),
        term_to_binary(wrapped_msg(Msg))}).

% Messages are NOT saved between AMQP broker restarts.
async_transient_publish(Topic, Msg) when is_list(Topic) ->
    async_transient_publish({undefined, Topic}, Msg);
async_transient_publish({Role, Topic}, Msg) ->
    gen_server:cast(name(Role), {transient_publish,
        iolist_to_binary(Topic),
        term_to_binary(wrapped_msg(Msg))}).

-record(chan, {
    broker    = undefined :: atom(),
    channel   = undefined :: pid(),
    mon       = undefined :: reference(),
    publisher = undefined :: {pid(), reference()},
    msg       = undefined :: {From :: gen_server:from(), Msg :: term()},
    active    = false     :: true | false
    }).

-record(state, {
    role,            % Queue role for precise publishing
    brokers = [],   % Defines order to send messages
    channels = [] :: [#chan{}], % Active AMQP Channels
    ch_timer = undefined, % Reconnection timer
    queue = queue:new()    % Excess queue
    }).

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

init({Role, Brokers}) ->
    CleanedBrokers = lists:usort(Brokers),
    State = #state{
                role = Role,
                brokers = Brokers,
                channels = [#chan{broker = B} || B <- CleanedBrokers]},
    {ok, lists:foldl(fun (B, S) -> reconnect(B, S) end, State, CleanedBrokers)}.

handle_call({publish, _Topic, _Binary} = PubMsg, From, State = #state{}) ->
    {noreply, drain_queue(State#state{
        queue = queue:in({From, PubMsg}, State#state.queue)
    }) };
handle_call({publish, _Exchange, _Topic, _Binary, _DeliveryMode, NoWait} = PubMsg, From, State = #state{}) ->
    if NoWait ->
        {reply, ok, drain_queue(State#state{
            queue = queue:in({nofrom, PubMsg}, State#state.queue)
            })};
       true ->
        {noreply, drain_queue(State#state{
            queue = queue:in({From, PubMsg}, State#state.queue)
            })}
    end;

handle_call({status}, _From, #state{queue = Q} = State) ->
    {reply, [{queue, queue:len(Q)}], State};

handle_call({state}, _From, State) ->
    {reply, State#state{queue = queue:new()}, State}.

handle_cast({publish, _Topic, _Binary} = PubMsg, State) ->
    {noreply, drain_queue(State#state{
        queue = queue:in({nofrom, PubMsg}, State#state.queue)
    }) };
handle_cast({transient_publish, _Topic, _Binary} = PubMsg, State = #state{}) ->
    {noreply, drain_queue(State#state{
        queue = queue:in({nofrom, PubMsg},
            State#state.queue)
    }) }.

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
        _ -> lager:warning("jamq_publisher(~p) terminating: ~1000000p", [self(), Reason])
    end,
    lists:foreach(
        fun (#chan{channel = Chan}) ->
            (Chan == undefined) orelse (catch lib_amqp:close_channel(Chan))
        end, Channels).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

format_status(_Opt, [_Dict, State]) ->
    [{data,  [{"State",  status_of_state(State)}]}].

%% INTERNAL FUNCTIONS

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


handle_down([], Ref, Reason, State = #state{role = Role, channels = Channels}) ->
    lager:error("JAMQ Publisher ~p(~p) unhandled 'DOWN' - ~p, reason ~100000000p / channels: ~10000000p", [Role, self(), Ref, Reason, Channels]),
    State;

handle_down([#chan{publisher = {_, Ref}, msg = {From, _}} = Chan|_], Ref, normal,
            #state{channels = Channels} = State) ->
    (From == nofrom) orelse gen_server:reply(From, ok),
    NewState = State#state{channels = clean_channel(Chan, Channels)},
    drain_queue(NewState);

handle_down([#chan{publisher = {_, Ref}, msg = Msg} = Chan|_], Ref, {{error, {channel, Reason}}, _},
            #state{channels = Channels, queue = Q, role = Role} = State) ->
    lager:info("Can not publish to channel on broker ~p: ~p", [Role, Reason]),
    NewState = State#state{queue = queue:in_r(Msg, Q), channels = clean_channel(Chan, Channels)},
    drain_queue(NewState);

handle_down([#chan{publisher = {_, Ref}, msg = Msg, broker = B} = Chan|_], Ref, _Reason,
            #state{channels = Channels, queue = Q} = State) ->
    NewState = State#state{queue = queue:in_r(Msg, Q), channels = clean_channel(Chan, Channels)},
    reconnect(B, NewState);

handle_down([#chan{mon = Ref, publisher = Publisher, msg = Msg, broker = B} = Chan|_], Ref, _Reason,
            #state{channels = Channels, queue = Q} = State) ->
    NewQ =
        case Publisher of
            {_, PMon} ->
                catch erlang:demonitor(PMon, [flush]),
                queue:in_r(Msg, Q);
            _ -> Q
        end,
    NewState = State#state{queue = NewQ, channels = clean_channel(Chan, Channels)},
    reconnect(B, NewState);
    
handle_down([_|Tail], Ref, Reason, State) ->
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

%% Kick off the next publishing action
drain_queue(#state{channels = Channels, brokers = Brokers, queue = Q} = State) ->
    case queue:out(Q) of
        {empty, _} -> State;
        {{value, {_From, _PubMsg} = Msg}, NewQ} ->
            case send_to_channels(Brokers, 0, Msg, Channels) of
                {ok, {N, NewChannels}}   -> drain_queue(State#state{channels = NewChannels, queue = NewQ, brokers = rotate_brokers(N, Brokers)});
                {error, no_channel} -> State
            end
    end;
drain_queue(State) -> State.

send_to_channels([], _, _, _) -> {error, no_channel};
send_to_channels([Next|Tail], N, {_, PubMsg} = Msg, Channels) ->
    case lists:keyfind(Next, #chan.broker, Channels) of
        #chan{channel = C, publisher = undefined, active = true} = Chan when is_pid(C) ->
            Publisher = spawn_monitor(
                            fun() ->
                                amqp_publish(C, PubMsg)
                            end),
            NewChan = Chan#chan{msg = Msg, publisher = Publisher},
            NewChannels = lists:keystore(Next, #chan.broker, Channels, NewChan),
            {ok, {N + 1, NewChannels}};
        _ ->
            send_to_channels(Tail, N + 1, Msg, Channels)
    end.

% Actually "Brokers" is always very small list, so there is no need to use queue or something
rotate_brokers(_, []) -> [];
rotate_brokers(0, L) -> L;
rotate_brokers(N, List) when N =< erlang:length(List) ->
    {L1, L2} = lists:split(N, List),
    L2 ++ L1.

amqp_publish(Channel, {publish, Topic, Binary}) ->
    amqp_publish(Channel, <<"jskit-bus">>, Topic, Binary,
        _DeliveryMode = 2);
amqp_publish(Channel, {transient_publish, Topic, Binary}) ->
    amqp_publish(Channel, <<"jskit-bus">>, Topic, Binary,
        _DeliveryMode = 1);
amqp_publish(Channel, {publish, Exchange, Topic, Binary, DeliveryMode, _NoWait}) ->
    amqp_publish(Channel, Exchange, Topic, Binary, DeliveryMode).

amqp_publish(Channel, Exchange, Topic, Binary, DeliveryMode) ->
    Res = lib_amqp:publish(Channel, Exchange, Topic, Binary,
        #'P_basic'{
            content_type = <<"application/octet-stream">>,
            delivery_mode = DeliveryMode,
            priority = 0
        }),
    case Res of
        closing -> erlang:error({error, {channel, closing}});
        blocked -> erlang:error({error, {channel, throttling}});
        ok -> ok;
        _ ->
            lager:error("Unexpected AMQ publish result: ~p", [Res]),
            ok
    end.

wrapped_msg(Msg) ->
    WrapInfo = [
        {published_timestamp, now()}
    ],
    {wrapped, WrapInfo, Msg}.

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

-define(EVENT_HANDLER_TO, 10000).

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

status_of_state(State) ->
    [
        {role, State#state.role},
        {queue_length, queue:len(State#state.queue)},
        {channels, State#state.channels}
    ].

