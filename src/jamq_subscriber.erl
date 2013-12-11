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

-define(gc_period, 1000).

-define(DEFAULT_EXCHANGE, <<"jskit-bus">>).
-define(RETRY_TIMEOUT, 5000).
-define(GRACEFUL_SHUTDOWN_TIMEOUT, 4500).

-include_lib("amqp_client/include/amqp_client.hrl").

% NOTE: Use jamq:unsubscribe/1 instead!
unsubscribe(ServerRef) -> gen_server:call(ServerRef, {unsubscribe}).

start_link(Properties) ->
    gen_server:start_link(?MODULE, Properties, []).

-record(subscription, {
    qname = node(),
    exchange = undefined,
    qdurable = false,
    qexclusive = true, % queue.declare exclusive (queue will be deleted after client disconnection)
    exclusive = false, % basic.consume exclusive
    qauto_delete = true,
    qbind_keys = [<<$#>>],
    topic = <<$#>>,
    broker = undefined,
    channel_properties = [],
    auto_ack = true,
    queue_args = [],
    status_callback = undefined,
    redelivery_ind = false
    }).

-record(state, {
    channel = undefined,
    function,
    messages = queue:new(),
    messages_retry_timer,
    message_processor,    % Process which handles messages.
    ch_monref,
    ch_timer,
    subscription = #subscription{},
    recv_msg_count = 0,
    supress_error = false,
    processes_waiting_for_unsubscribe = [] % when this list is non-empty
                                           % we wait for message_processor to finish
                                           % and don't launch new ones
    }).

init(Properties) ->

    erlang:process_flag(trap_exit, true),

    [Dur, Exc, SubExc, AutoD, T, Q, QBT, QBKeys, Br, Ex, F, AutoAck, QArgs, StatusCallback, SupressError, ConnectDelay, RedeliveryInd]
        = [proplists:get_value(K, Properties, D) || {K, D} <- [
            {durable, undefined},
            {exclusive, undefined},
            {subscribe_exclusive, false},
            {auto_delete, undefined},
            {topic, <<$#>>},
            {queue, transient},
            {queue_bind_tag, undefined}, % deprecated, use queue_bind_keys
            {queue_bind_keys, []},
            {broker, undefined},
            {exchange, ?DEFAULT_EXCHANGE},
            {function,
                fun(Msg) ->
                    lager:error("Unhandled amqp message: ~p", [Msg])
                end},
            {auto_ack, true},
            {queue_args, []},
            {status_callback, undefined},
            {supress_error, false},
            {connect_delay, undefined},
            {redelivery_ind, false}
        ] ],

    ChannelProps = [{K, P} ||
        K <- [prefetch_count],
        P <- [proplists:get_value(K, Properties)],
        P /= undefined],

    Topic = iolist_to_binary(T),
    {{Durable, Exclusive, AutoDelete}, QName, QBindTag} = if
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
        QBT == undefined andalso QBKeys == []  -> throw({missing, queue_bind_keys});
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
                    exclusive = SubExc,
                    qauto_delete = FinalAutoDeleteQ,
                    qbind_keys = [iolist_to_binary(K) || K <- [QBindTag] ++ QBKeys, K /= undefined],
                    topic = Topic,
                    broker = Br,
                    channel_properties = ChannelProps,
                    auto_ack = AutoAck,
                    queue_args = QArgs,
                    status_callback = StatusCallback,
                    redelivery_ind = RedeliveryInd
                },

    (ConnectDelay < ?RETRY_TIMEOUT) andalso erlang:send_after(ConnectDelay, self(), acquire_channel),

    {ok, #state{
        function = F,
        subscription = SubscrDef,
        ch_timer = start_acquire_channel_timer(),
        supress_error = SupressError
    } }.

handle_call({unsubscribe}, From,
            #state{message_processor = MessageProcessor,
                   processes_waiting_for_unsubscribe = Unsubscribers} = OldState) ->
    case MessageProcessor of
        undefined ->
            % There is no worker process, we can shut down right now.
            State = unsubscribe_and_close(unsubscribe, OldState),
            {stop,
             normal,
             {ok, {unsubscribed,
                 (State#state.subscription)#subscription.topic}},
             State};
        _ ->
            % Let's wait for worker process to finish
            case Unsubscribers of
                [] -> {ok, _TimerRef} = timer:send_after(?GRACEFUL_SHUTDOWN_TIMEOUT, force_shutdown);
                _ -> ok % Shutdown timer has already been created when we got the first 'unsubscribe' call
            end,
            % We will reply either after message_processor finishes or after timeout.
            {noreply, OldState#state{processes_waiting_for_unsubscribe = [From | Unsubscribers]}}
    end.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({#'basic.deliver'{delivery_tag = DeliveryTag, redelivered = Redelivered},
        #amqp_msg{payload = Payload}},
    #state{channel = Channel, messages = MsgsQ} = State) when is_pid(Channel) ->
    {noreply, dispatch(State#state{
            messages = queue:in({DeliveryTag, Redelivered, Payload}, MsgsQ)
        })};
handle_info(#'basic.consume_ok'{}, State) -> {noreply, State};
handle_info(#'basic.qos_ok'{}, State) -> {noreply, State};
handle_info({#'basic.deliver'{}, _},
    #state{channel = undefined} = State) -> {noreply, State};
handle_info(acquire_channel, #state{channel = undefined,
        supress_error = SupressError,
        subscription = #subscription{
                broker = BrokerRole,
                qname = QName,
                topic = Topic,
                exclusive = Exc,
                status_callback = StatusCallback
            }
        } = State) ->
    SupressError orelse lager:info("AMQ subscriber ~p: acquire_channel timer expired", [self()]),
    NewState = try jamq_channel:channel(BrokerRole) of
        Channel ->
            try
                setup_queue(Channel, State),
                lib_amqp:subscribe(Channel, QName, self(), Topic, false, Exc),
                lager:info("Subscribed to ~p through ~p", [QName, Channel]),
                timer:cancel(State#state.ch_timer),
                (StatusCallback =/= undefined) andalso StatusCallback(up),
                State#state{channel = Channel,
                    ch_monref = erlang:monitor(process, Channel),
                    ch_timer = undefined }
            catch
                _:SubError ->
                    catch lib_amqp:close_channel(Channel),
                    SupressError orelse lager:error("Subscription to ~p failed: ~p", [QName, SubError]),
                    (StatusCallback =/= undefined) andalso StatusCallback(down),
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
        subscription = #subscription{auto_ack = AutoAck,
                                     topic = Topic}} = State) ->
    {{value, {DeliveryTag, _, _Payload}}, NewMsgsQ} = queue:out(MsgsQ),
    case AutoAck of
        true -> lib_amqp:ack(Channel, DeliveryTag);
        false -> ok
    end,
    case State#state.processes_waiting_for_unsubscribe of
        [] -> {noreply, dispatch(on_msg_received(State#state{messages = NewMsgsQ,
                                                             message_processor = undefined}))};
        Clients ->
            NextState = unsubscribe_and_close(unsubscribe, State),
            [finish_unsubscribe(From, Topic) || From <- Clients],
            {stop, normal, NextState#state{message_processor = undefined,
                                           processes_waiting_for_unsubscribe = []}}
    end;
%% Message was handled improperly, start the retry timer and notify the user.
handle_info({'DOWN', ERef, process, EPid, Info},
        #state{message_processor = {EPid,ERef}} = State) ->
    {noreply, retry_handler(Info, State)};
handle_info(retry_dispatch, #state{messages_retry_timer = T} = State) when T /= undefined ->
    {noreply, dispatch(State#state{messages_retry_timer = undefined})};
handle_info({'DOWN', MRef, process, ChanPid, _Info},
        #state{channel = ChanPid, ch_monref = MRef,
                messages_retry_timer = RetryTimer,
                subscription = #subscription{status_callback = StatusCallback}
            } = State) ->

    (StatusCallback =/= undefined) andalso StatusCallback(down),

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

handle_info({'EXIT', _From, normal}, State) ->
    {noreply, State};

handle_info({'EXIT', _From, Reason}, State) ->
    {stop, Reason, unsubscribe_and_close(unexpected_exit, State)};

handle_info(force_shutdown,
            #state{processes_waiting_for_unsubscribe = Clients,
                   subscription = #subscription{topic = Topic}} = State) ->
    NextState = unsubscribe_and_close(unsubscribe, State),
    [finish_unsubscribe(From, Topic) || From <- Clients],
    {stop,
     normal,
     NextState#state{
            message_processor = undefined,
            processes_waiting_for_unsubscribe = []
            }};

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

finish_unsubscribe(From, Topic) ->
    gen_server:reply(From, {ok, {unsubscribed, Topic}}),
    ok.

terminate(Reason, State) -> unsubscribe_and_close(Reason, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


format_status(_Opt, [_Dict, State]) ->
    [{data,  [{"State",  status_of_state(State)}]}].

%% INTERNAL FUNCTIONS

unsubscribe_and_close(_Reason, #state{channel = undefined} = State) -> State;
unsubscribe_and_close(_Reason, #state{channel = Channel} = State) ->
    QBKeys = (State#state.subscription)#subscription.qbind_keys,
    Topic = (State#state.subscription)#subscription.topic,
    StatusCallback = (State#state.subscription)#subscription.status_callback,
    A = lib_amqp:unsubscribe(Channel, Topic),
    B = lib_amqp:close_channel(Channel),
    lager:info("Unsubscribed from ~p ~p: {~p, ~p} because of exit ~p",
        [QBKeys, Topic, A, B, _Reason]),
    (StatusCallback =/= undefined) andalso StatusCallback(down),
    State#state{channel = undefined}.

setup_queue(Channel, #state{ subscription = #subscription{
                                                qname = QueueName,
                                                exchange = ExName,
                                                qdurable = Dur,
                                                qexclusive = Exc,
                                                qauto_delete = AutoD,
                                                qbind_keys = QBindKeys,
                                                channel_properties = ChannelProps,
                                                queue_args = Args}}) ->
    jamq_api:declare_queue(Channel, QueueName, Dur, Exc, AutoD, Args),

    [{'queue.bind_ok'} = lib_amqp:bind_queue(Channel, ExName, QueueName, K) || K <- QBindKeys],

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
dispatch(#state{function = Callback,
        messages = MsgsQ, messages_retry_timer = undefined,
        message_processor = undefined,
        channel = Channel,
               subscription = #subscription{ auto_ack = AutoAck, redelivery_ind = NeedRedelInd}} = State) ->
    case queue:out(MsgsQ) of
        {empty, _} -> State;
        {{value, {DeliveryTag, Redelivered, Payload}}, _ShorterMsgsQ} ->
            AutoAckArgs = case AutoAck of
                true -> [];
                false -> [Channel, DeliveryTag]
            end,

            RedelArgs =
                case NeedRedelInd of
                    true  -> [Redelivered];
                    false -> []
                end,

            {Info, Message} =
                case binary_to_term(Payload) of
                    {wrapped, I, Msg} -> {I, Msg};
                    Msg -> {[], Msg}
                end,

            NF = fun() ->
                    lists:map(fun process_wrapped_info/1, Info),
                    case Callback of
                        {M,F,A} -> apply(M, F, A ++ [Message | RedelArgs ++ AutoAckArgs]);
                        _ when is_function(Callback) -> erlang:apply(Callback, [Message | RedelArgs ++ AutoAckArgs])
                    end
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
        {bindkeys, (State#state.subscription)#subscription.qbind_keys},
        {topic, (State#state.subscription)#subscription.topic},
        {broker, (State#state.subscription)#subscription.broker},
        {proc, State#state.message_processor},
        {recv_message_count, State#state.recv_msg_count}
    ].

start_acquire_channel_timer() ->
    {ok, TRef} = timer:send_interval(?RETRY_TIMEOUT, acquire_channel),
    TRef.

retry_handler(retry_message = Info, State) ->
    retry_handler(State, warning, Info, 300);
retry_handler(Info, State) ->
    retry_handler(State, error, Info, 15000 + random:uniform(30000)).

retry_handler(#state{messages = MsgsQ} = State, MessageType, Info, RetryInterval) ->
    {_DeliveryTag, _, Payload} = queue:get(MsgsQ),
    lager:log(MessageType, [], "Processing message ~P in queue ~p failed: ~p",
        [binary_to_term(Payload), 5,
        (State#state.subscription)#subscription.qname,
        Info]),
    {ok, Timer} = timer:send_after(RetryInterval, retry_dispatch),
    State#state{message_processor = undefined, messages_retry_timer = Timer}.

on_msg_received(State = #state{recv_msg_count = N}) ->
    (N rem ?gc_period) == 0 andalso erlang:garbage_collect(),
    State#state{recv_msg_count = N + 1}.


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
    kill_all_subscribers_sup(4000).

kill_all_subscribers_sup(Timeout) ->
    Subs = all_subscribers_sups(),
    io:format("Going to stop ~p subscriber supervisors~n", [erlang:length(Subs)]),
    [begin
        catch jamq_subscriber_sup:stop(P),
        catch erlang:exit(P, kill),
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

