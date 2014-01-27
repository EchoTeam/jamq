%%
%% Top level API for use AMQ at the application level.
%%
%% Documentation:
%% https://trac.jacknyfe.net/projects/jacknyfe/wiki/MessagingAPI
%% vim: set ts=4 sts=4 sw=4 et:
%%
-module(jamq).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([
    create_queue/1,             % BC, use create_queue/2
    create_queue/2,             % Create a queue
    delete_queue/1,             % BC, use delete_queue/2,3
    delete_queue/2,             % Delete a queue
    delete_queue/3,             % Delete a queue
    publish/2,                  % Publish something to an AMQ server.
    publish/3,                  % Publish something to an AMQ server, with a timeout.
    publish_by_key/3,           % Publish something to an AMQ server by key.
    publish_by_key/4,           % Publish something to an AMQ server by key, with a timeout.
    async_publish/2,            % Publish something without waiting for confirmation.
    async_publish_by_key/3,     % Publish something without waiting for confirmation by key.
    subscribe/1,                % Subscribe to a topic, returns {ok, ServerRef}
    subscribe/2,
    start_subscriber/1,
    stop_subscriber/1,
    sync_request/2,
    sync_request/3,
    unsubscribe/1               % Unsubscribe from a topic (takes ServerRef)
]).


-define(DEFAULT_SYNC_REQUEST_TIMEOUT, 10000).

-type server_name() :: {local, atom()} | {global, term()} | {via, atom(), term()}.
-type topic() :: iolist() | binary().
-type subscriber_opt() :: {server_name, server_name()}
                        | {durable, boolean()}
                        | {exclusive, boolean()}
                        | {subscribe_exclusive, boolean()}
                        | {auto_delete, boolean()}
                        | {topic, topic()}
                        | {queue, transient | atom() | topic()}
                        | {queue_bind_tag, topic()} % XXX: deprecated, use queue_bind_keys
                        | {queue_bind_keys, list(topic())}
                        | {broker, atom()}
                        | {exchange, binary()}
                        | {function, fun()}
                        | {auto_ack, boolean()}
                        | {queue_args, list()}
                        | {status_callback, undefined | fun()}
                        | {supress_error, boolean()}
                        | {connect_delay, integer()}
                        | {redelivery_ind, boolean()}.
-type subscriber_opts() :: [subscriber_opt()].
-type subscribe_ret() :: {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.


publish(Topic, Msg) -> jamq_publisher:publish(Topic, Msg).
publish(Topic, Msg, Timeout) -> jamq_publisher:publish(Topic, Msg, Timeout).
publish_by_key(Topic, Msg, Key) -> jamq_publisher:publish_by_key(Topic, Msg, Key).
publish_by_key(Topic, Msg, Key, Timeout) -> jamq_publisher:publish_by_key(Topic, Msg, Key, Timeout).

async_publish(Topic, Msg) -> jamq_publisher:async_publish(Topic, Msg).
async_publish_by_key(Topic, Msg, Key) -> jamq_publisher:async_publish_by_key(Topic, Msg, Key).

%% @see subscribe/1
-spec subscribe(topic(), fun()) -> subscribe_ret().
subscribe(Topic, Fun) ->
    subscribe([{topic, Topic},{function, Fun}]).

-spec subscribe(topic() | subscriber_opts()) -> subscribe_ret().
subscribe([Option|_] = Options) when is_tuple(Option) ->
    jamq_client_mon:start_link(Options);

subscribe(Topic) when is_list(Topic); is_binary(Topic) ->
    subscribe([{topic, Topic}]).

unsubscribe(Ref) ->
    jamq_client_mon:stop(Ref).

start_subscriber(Options) ->
    jamq_subscriber_top_sup:start_subscriber(Options).

stop_subscriber(Ref) ->
    jamq_subscriber_top_sup:stop_subscriber(Ref).

% Use create_queue/2 instead
create_queue({BrokerRole, QueueName}) ->
    create_queue(BrokerRole, QueueName).

create_queue(BrokerRole, QueueName) when is_atom(BrokerRole), is_list(QueueName) ->
    jsk_async:complete(
        fun() ->
            Brokers = jamq_api:get_brokers(BrokerRole),
            lists:foreach(
                fun (B) ->
                    Channel = jamq_channel:channel(jamq_channel:name(BrokerRole, B)),
                    Queue = list_to_binary(QueueName),
                    jamq_api:declare_permanent_queue(Channel, Queue),
                    #'queue.bind_ok'{} =
                        jamq_api:bind_queue(Channel, <<"jskit-bus">>, Queue, Queue)
                end, Brokers),
            ok
        end).

%% Use delete_queue/2 instead
delete_queue({BrokerRole, Q}) ->
    delete_queue(BrokerRole, Q).

delete_queue(BrokerRole, Q) ->
    delete_queue(BrokerRole, Q, []).

delete_queue(BrokerRole, Q, Options) when is_atom(BrokerRole), is_list(Q), is_list(Options) ->
    jsk_async:complete(
        fun () ->
            Brokers = jamq_api:get_brokers(BrokerRole),
            lists:foreach(
                fun (B) ->
                    Channel = jamq_channel:channel(jamq_channel:name(BrokerRole, B)),
                    try
                        #'queue.delete_ok'{} = jamq_api:delete_queue(Channel, Q, Options)
                    catch
                        exit:{{shutdown, {server_initiated_close,404, _}}, _} -> ok
                    end
                end, Brokers),
            ok
        end).

sync_request(Topic, Msg) ->
    sync_request(Topic, Msg, ?DEFAULT_SYNC_REQUEST_TIMEOUT).
sync_request(Topic, Msg, Timeout) ->
    Self = self(),
    RequestRef = make_ref(),
    publish(Topic, {{Self, RequestRef, Timeout}, Msg}),
    receive
        {reply, RequestRef, Value} -> Value
    after Timeout ->
        {error, timeout}
    end.
