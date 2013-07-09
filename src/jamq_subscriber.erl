%%
%% Subscriber to an AMQ messages stream.
%%
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber).

-behavior(gen_server).

-export([
    start_link/1,
    unsubscribe/1,
    all_subscribers/0,
    all_subscribers_sups/0,
    kill_all_subscribers_sup/1,
    kill_all_subscribers_sup/0,
    kill_all_subscribers/2
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

-define(DEFAULT_EXCHANGE, <<"jskit-bus">>).

-include_lib("rabbitmqclient/include/amqp_client.hrl").

% NOTE: Use jamq:unsubscribe/1 instead!
unsubscribe(ServerRef) -> gen_server:call(ServerRef, {unsubscribe}).

start_link(Properties) ->
    gen_server:start_link(?MODULE, Properties, []).

-record(subscription, {
    qname = node(),
    exchange = undefined,
    qdurable = false,
    qexclusive = true,
    qauto_delete = true,
    qbind_tag = <<$#>>,
    topic = <<$#>>,
    broker = undefined,
    channel_properties = [],
    auto_ack = true
    }).

-record(state, {
    channel = undefined,
    function,
    messages = queue:new(),
    messages_retry_timer,
    message_processor,    % Process which hangles messages.
    ch_monref,
    ch_timer,
    subscription = #subscription{}
    }).

init(Properties) ->
    [Dur, Exc, AutoD, T, Q, QBT, Br, Ex, F, AutoAck]
        = [proplists:get_value(K, Properties, D) || {K, D} <- [
            {durable, undefined},
            {exclusive, undefined},
            {auto_delete, undefined},
            {topic, <<$#>>},
            {queue, transient},
            {queue_bind_tag, undefined},
            {broker, undefined},
            {exchange, ?DEFAULT_EXCHANGE},
            {function, fun(Msg) ->
            lager:info("Message received: ~p", [Msg])
            end},
            {auto_ack, true}
        ] ],
    ChannelProps = [{K, P} ||
        K <- [prefetch_count],
        P <- [proplists:get_value(K, Properties)],
        P /= undefined],
    Topic = iolist_to_binary(T),
    {{Durable, Exclusive, AutoDelete},
            QName, QBindTag} = if
        Q == transient ->
            TmpQBindTag = case QBT of
              undefined ->
                % Transient queue bind tag is the same as
                % subscribe template tag.
                Topic;
              _ -> QBT
            end,
            {{false, true, true},
            % Transient queues have descriptive names:
            % <node>-<pid>-<topic>
            iolist_to_binary([atom_to_list(node()),
                "-", string:tokens(pid_to_list(self()), "<>"),
                "-", TmpQBindTag]),
            TmpQBindTag};
        QBT == undefined -> throw({missing, queue_bind_tag});
        Br == undefined -> throw({missing, broker});
        is_atom(Q) -> {{true, false, false}, atom_to_list(Q), QBT};
        true -> {{true, false, false}, iolist_to_binary(Q), QBT}
    end,

    erlang:is_binary(Ex) orelse throw({invalid_exchange, Ex}),

    % Final resolution of suggestions
    FinalDurableQ = if Dur == undefined -> Durable; true -> Dur end,
    FinalExclusiveQ = if Exc == undefined -> Exclusive; true -> Exc end,
    FinalAutoDeleteQ = if AutoD == undefined -> AutoDelete; true -> AutoD end,

        SubscrDef = #subscription{
                exchange = Ex,
                qname = QName,
                qdurable = FinalDurableQ,
                qexclusive = FinalExclusiveQ,
                qauto_delete = FinalAutoDeleteQ,
                qbind_tag = iolist_to_binary(QBindTag),
                topic = Topic,
                broker = Br,
                channel_properties = ChannelProps,
                auto_ack = AutoAck
        },

    {ok, #state{
        function = F,
        subscription = SubscrDef,
        ch_timer = start_acquire_channel_timer()
    } }.

handle_call({unsubscribe}, _From, OldState) ->
    State = unsubscribe_and_close(unsubscribe, OldState),
    {stop, normal,
        {ok, {unsubscribed,
            (State#state.subscription)#subscription.topic}},
    State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({#'basic.deliver'{delivery_tag = DeliveryTag},
        #amqp_msg{payload = Payload}},
    #state{channel = Channel, messages = MsgsQ} = State) when is_pid(Channel) ->
    {noreply, dispatch(State#state{
            messages = queue:in({DeliveryTag, Payload}, MsgsQ)
        })};
handle_info(#'basic.consume_ok'{}, State) -> {noreply, State};
handle_info(#'basic.qos_ok'{}, State) -> {noreply, State};
handle_info({#'basic.deliver'{}, _},
    #state{channel = undefined} = State) -> {noreply, State};
handle_info(acquire_channel, #state{channel = undefined,
        subscription = #subscription{
                broker = BrokerRole,
                qname = QName,
                topic = Topic
            }
        } = State) ->
    lager:info("AMQ subscriber ~p: acquire_channel timer expired", [self()]),
    NewState = try jamq_channel:channel(BrokerRole) of
        Channel ->
            try
                setup_queue(Channel, State),
                lib_amqp:subscribe(Channel, QName, self(), Topic, false),
                lager:info("Subscribed to ~p through ~p", [QName, Channel]),
                timer:cancel(State#state.ch_timer),
                State#state{channel = Channel,
                    ch_monref = erlang:monitor(process, Channel),
                    ch_timer = undefined }
            catch
                _:SubError ->
                    catch lib_amqp:close_channel(Channel),
                    lager:error("Subscription to ~p failed: ~p", [QName, SubError]),
                    State
            end
    catch _:_ -> State
    end,
    {noreply, NewState};
%% When message processor is down 'normal', it means the message was properly
%% handled. Remove the message from the queue and acknowledge receipt.
handle_info({'DOWN', ERef, process, EPid, normal},
        #state{channel = Channel, messages = MsgsQ,
        message_processor = {EPid,ERef},
        subscription = #subscription{ auto_ack = AutoAck }} = State) ->
    {{value, {DeliveryTag, _Payload}}, NewMsgsQ} = queue:out(MsgsQ),
    case AutoAck of
        true -> lib_amqp:ack(Channel, DeliveryTag);
        false -> ok
    end,
    {noreply, dispatch(State#state{messages = NewMsgsQ,
        message_processor = undefined})};
%% Message was handled improperly, start the retry timer and notify the user.
handle_info({'DOWN', ERef, process, EPid, Info},
        #state{message_processor = {EPid,ERef}} = State) ->
    {noreply, retry_handler(Info, State)};
handle_info(retry_dispatch, #state{messages_retry_timer = T} = State) when T /= undefined ->
    {noreply, dispatch(State#state{messages_retry_timer = undefined})};
handle_info({'DOWN', MRef, process, ChanPid, _Info},
        #state{channel = ChanPid, ch_monref = MRef,
                messages_retry_timer = RetryTimer
            } = State) ->
    case RetryTimer of
        undefined -> ok;
        _ -> erlang:cancel_timer(RetryTimer)
    end,
    {noreply,
        State#state{
            channel = undefined,
            ch_timer = start_acquire_channel_timer(),
            ch_monref = undefined,
            messages = queue:new(),
            message_processor = undefined,
            messages_retry_timer = undefined
        }};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(Reason, State) -> unsubscribe_and_close(Reason, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


format_status(_Opt, [_Dict, State]) ->
    [{data,  [{"State",  status_of_state(State)}]}].

%% INTERNAL FUNCTIONS

unsubscribe_and_close(_Reason, #state{channel = undefined} = State) -> State;
unsubscribe_and_close(_Reason, #state{channel = Channel} = State) ->
    QBTag = (State#state.subscription)#subscription.qbind_tag,
    Topic = (State#state.subscription)#subscription.topic,
    A = lib_amqp:unsubscribe(Channel, Topic),
    B = lib_amqp:close_channel(Channel),
    lager:info("Unsubscribed from ~p ~p: {~p, ~p} because of exit ~p",
        [QBTag, Topic, A, B, _Reason]),
    State#state{channel = undefined}.

setup_queue(Channel, #state{ subscription = #subscription{
                                                qname = QueueName,
                                                exchange = ExName,
                                                qdurable = Dur,
                                                qexclusive = Exc,
                                                qauto_delete = AutoD,
                                                qbind_tag = QBindTag,
                                                channel_properties = ChannelProps}}) ->
    jamq_api:declare_queue(Channel, QueueName, Dur, Exc, AutoD),

    {'queue.bind_ok'} = lib_amqp:bind_queue(Channel, ExName, QueueName, QBindTag),

    case proplists:get_value(prefetch_count, ChannelProps) of
        undefined -> lib_amqp:set_prefetch_count(Channel, 1000);
        N -> lib_amqp:set_prefetch_count(Channel, N)
    end,
    amqp_channel:cast(Channel, {'basic.recover', true}),
    ok.

%% Dispatch function initiates evaluation of the message in a separate process.
%% Using a separate process ensures that this server does not accumulate
%% garbage during processing, such as leaked process-bound locks, channels,
%% dictionary items, and stuff.
dispatch(#state{function = F,
        messages = MsgsQ, messages_retry_timer = undefined,
        message_processor = undefined,
        channel = Channel,
               subscription = #subscription{ auto_ack = AutoAck }} = State) ->
    case queue:out(MsgsQ) of
      {empty, _} -> State;
      {{value, {DeliveryTag, Payload}}, _ShorterMsgsQ} ->
          RestOfArgs = case AutoAck of
              true -> [];
              false -> [Channel, DeliveryTag]
          end,
          NF = case binary_to_term(Payload) of
              {wrapped, Info, M} ->
                  fun() ->
                          lists:map(fun process_wrapped_info/1, Info),
                          apply(F, [M | RestOfArgs])
                  end;
              AM ->
                  fun() -> apply(F, [AM | RestOfArgs]) end
          end,
          State#state{message_processor = spawn_monitor(NF)}
    end;
dispatch(#state{} = State) -> State.

process_wrapped_info({published_timestamp, _Ts}) -> nop;
process_wrapped_info(_) -> ok.

status_of_state(State) ->
    [
        {queue_length, queue:len(State#state.messages)},
        {qname, (State#state.subscription)#subscription.qname},
        {bindtag, (State#state.subscription)#subscription.qbind_tag},
        {topic, (State#state.subscription)#subscription.topic},
        {broker, (State#state.subscription)#subscription.broker},
        {proc, State#state.message_processor}
    ].

start_acquire_channel_timer() ->
    {ok, TRef} = timer:send_interval(5000, acquire_channel),
    TRef.

retry_handler(retry_message = Info, State) ->
    retry_handler(State, warning, Info, 300);
retry_handler(Info, State) ->
    retry_handler(State, error, Info, 15000 + random:uniform(30000)).

retry_handler(#state{messages = MsgsQ} = State, MessageType, Info, RetryInterval) ->
    {_DeliveryTag, Payload} = queue:get(MsgsQ),
    lager:log(MessageType, [], "Processing message ~P in queue ~p failed: ~p",
        [binary_to_term(Payload), 3,
        (State#state.subscription)#subscription.qname,
        Info]),
    {ok, Timer} = timer:send_after(RetryInterval, retry_dispatch),
    State#state{message_processor = undefined, messages_retry_timer = Timer}.



% Helps restart subscribers after code update
all_subscribers() ->
    processes_by_initial_call({jamq_subscriber, init, 1}).

all_subscribers_sups() ->
    processes_by_initial_call({supervisor, jamq_subscriber_sup, 1}).

processes_by_initial_call(InitialCall) ->
    Procs = erlang:processes(),
    lists:foldl(
        fun (P, Acc) ->
            try
                Dictionary = element(2, erlang:process_info(P, dictionary)),
                IC = (catch proplists:get_value('$initial_call', Dictionary)),
                case InitialCall == IC of
                    true  -> [P|Acc];
                    false -> Acc
                end
            catch
                _:_ -> Acc
            end
        end, [], Procs).

kill_all_subscribers_sup() ->
    kill_all_subscribers_sup(2000).

kill_all_subscribers_sup(Timeout) ->
    Subs = all_subscribers_sups(),
    io:format("Going to stop ~p subscriber supervisors~n", [erlang:length(Subs)]),
    [begin
        jamq_subscriber_sup:stop(P),
        io:format("."),
        timer:sleep(Timeout)
    end||P <- Subs].

kill_all_subscribers(Reason, Timeout) ->
    Subs = all_subscribers(),
    io:format("Going to stop ~p subscribers~n", [erlang:length(Subs)]),
    [begin
        erlang:exit(P, Reason),
        io:format("."),
        timer:sleep(Timeout)
    end||P <- Subs].

