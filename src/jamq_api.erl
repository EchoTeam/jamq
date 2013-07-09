%%
%% This is a low-level bridge to RabbitMQ native Erlang library.
%% Don't use it directly, use API provided by jamq.erl module.
%% vim: set ts=4 sts=4 sw=4 et:
%%
-module(jamq_api).
-compile(export_all).

-include_lib("rabbitmqclient/include/amqp_client.hrl").

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
    lib_amqp:declare_queue(Channel, #'queue.declare'{
        queue = Q,
        passive = false,
        durable = Durable,
        exclusive = Exclusive,
        auto_delete = AutoDelete,
        nowait = false,
        arguments = []
    }).

delete_queue({BrokerRole, Q}) -> delete_queue(jamq:channel(BrokerRole), Q).

delete_queue(Channel, Q) -> 
        lib_amqp:delete_queue(Channel, type_utils:to_binary(Q)).

publish(Channel, Message) ->
    lib_amqp:publish(Channel, <<"jskit-bus">>,
        <<"update">>,
        term_to_binary({jamq, Message})).

peek(Channel) -> peek(Channel, list_to_binary(atom_to_list(node()))).

peek(Channel, Q) ->
    lib_amqp:get(Channel, Q, false).

get(Channel) -> get(Channel, list_to_binary(atom_to_list(node()))).

get(Channel, Q) ->
    lib_amqp:get(Channel, Q, true).
