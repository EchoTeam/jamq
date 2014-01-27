%%
%% This is a low-level bridge to RabbitMQ native Erlang library.
%% Don't use it directly, use API provided by jamq.erl module.
%% vim: set ts=4 sts=4 sw=4 et:
%%
-module(jamq_api).
-compile(export_all).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(DEFAULT_AMQP_HEARTBEAT, 10). % in sec

start_connection() ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    Connection.

start_connection(Host) ->
    start_connection(Host, ?PROTOCOL_PORT).

start_connection(Host, Port) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{
            host = Host,
            port = Port,
            heartbeat = ?DEFAULT_AMQP_HEARTBEAT
        }),
    Connection.

start_channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

get_brokers(Broker) -> jamq_supervisor:get_brokers(Broker).

declare_permanent_exchange(Channel, X, Type) ->
    amqp_channel:call(Channel, #'exchange.declare'{
        exchange = X,
        type = Type,
        passive = false,
        durable = true,
        auto_delete = false,
        internal = false,
        nowait = false,
        arguments = []
    }).

delete_exchange(Channel, X) ->
    ExchangeDelete = #'exchange.delete'{exchange = X,
        if_unused = true, nowait = false},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete).

declare_permanent_queue(Channel, Q) ->
    % Durable exclusive queues are not persistent,
    % so we declare durable non-exclusive one.
    declare_queue(Channel, Q, true, false, false).

declare_transient_queue(Channel, Q) ->
    declare_queue(Channel, Q, false, true, true).


declare_queue(Channel, Q, Durable, Exclusive, AutoDelete) ->
    declare_queue(Channel, Q, Durable, Exclusive, AutoDelete, []).

declare_queue(Channel, Q, Durable, Exclusive, AutoDelete, Args) ->
    declare_queue(Channel, #'queue.declare'{
        queue = Q,
        passive = false,
        durable = Durable,
        exclusive = Exclusive,
        auto_delete = AutoDelete,
        nowait = false,
        arguments = Args
    }).

declare_queue(Channel) ->
    declare_queue(Channel, <<>>).

declare_queue(Channel, QueueDeclare = #'queue.declare'{}) ->
    #'queue.declare_ok'{queue = QueueName}
        = amqp_channel:call(Channel, QueueDeclare),
    QueueName;

declare_queue(Channel, Q) ->
    %% TODO Specifying these defaults is unecessary - this is already taken
    %% care of in the spec file
    QueueDeclare = #'queue.declare'{queue = Q},
    declare_queue(Channel, QueueDeclare).


bind_queue(Channel, X, Q, Binding) ->
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = Binding},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

delete_queue(Channel, Q) ->
    delete_queue(Channel, Q).

delete_queue(Channel, Q, Options) ->
    QueueDelete =
        lists:foldl(
            fun ({if_unused, Val}, D) -> D#'queue.delete'{if_unused = Val};
                ({if_empty, Val}, D) -> D#'queue.delete'{if_empty = Val}
            end, #'queue.delete'{queue = Q}, Options),
    #'queue.delete_ok'{} = amqp_channel:call(Channel, QueueDelete).

%% Sets the prefetch count for messages delivered on this channel
set_prefetch_count(Channel, Prefetch) ->
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch}).

publish(Channel, Message) ->
    publish(Channel, <<"jskit-bus">>,
        <<"update">>,
        term_to_binary({jamq, Message})).

publish(Channel, X, RoutingKey, Payload) ->
    publish(Channel, X, RoutingKey, Payload, false).

publish(Channel, X, RoutingKey, Payload, Mandatory) when is_boolean(Mandatory)->
    BP = #'P_basic'{content_type = <<"application/octet-stream">>,
                    delivery_mode = 1,
                    priority = 0},
    publish(Channel, X, RoutingKey, Payload, Mandatory, BP);

publish(Channel, X, RoutingKey, Payload, Properties) ->
    publish(Channel, X, RoutingKey, Payload, false, Properties).

publish(Channel, X, RoutingKey, Payload, Mandatory, Properties) ->
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = Mandatory},
    Content = #amqp_msg{props = Properties, payload = Payload},
    amqp_channel:call(Channel, BasicPublish, Content).

subscribe(Channel, Q, Consumer) ->
    subscribe(Channel, Q, Consumer, <<>>, true).

subscribe(Channel, Q, Consumer, NoAck) when is_boolean(NoAck) ->
    subscribe(Channel, Q, Consumer, <<>>, NoAck);

subscribe(Channel, Q, Consumer, Tag) ->
    subscribe(Channel, Q, Consumer, Tag, true).

subscribe(Channel, Q, Consumer, Tag, NoAck) ->
    subscribe(Channel, Q, Consumer, Tag, NoAck, false).

subscribe(Channel, Q, Consumer, Tag, NoAck, Exclusive) ->
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = Tag,
                                    no_ack = NoAck,
                                    exclusive = Exclusive},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, BasicConsume, Consumer),
    ConsumerTag.

ack(Channel, DeliveryTag) ->
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    ok = amqp_channel:cast(Channel, BasicAck).

unsubscribe(Channel, Tag) ->
    BasicCancel = #'basic.cancel'{consumer_tag = Tag},
    #'basic.cancel_ok'{} = amqp_channel:call(Channel, BasicCancel),
    ok.


close_channel(Channel) ->
    amqp_channel:close(Channel),
    ok.

close_connection(Connection) ->
    amqp_connection:close(Connection),
    ok.

peek(Channel) -> peek(Channel, list_to_binary(atom_to_list(node()))).

peek(Channel, Q) ->
    get(Channel, Q, false).

get(Channel) -> get(Channel, list_to_binary(atom_to_list(node()))).

get(Channel, Q) -> get(Channel, Q, true).

get(Channel, Q, NoAck) ->
    BasicGet = #'basic.get'{queue = Q, no_ack = NoAck},
    {Method, Content} = amqp_channel:call(Channel, BasicGet),
    case Method of
        'basic.get_empty' -> 'basic.get_empty';
        _ ->
            #'basic.get_ok'{delivery_tag = DeliveryTag} = Method,
            case NoAck of
                true -> Content;
                false -> {DeliveryTag, Content}
            end
    end.
