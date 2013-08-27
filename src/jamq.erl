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
    publish/2,                  % Publish something to an AMQ server.
    publish/3,                  % Publish something to an AMQ server, with a timeout.
    publish_by_key/3,           % Publish something to an AMQ server by key.
    publish_by_key/4,           % Publish something to an AMQ server by key, with a timeout.
    async_publish/2,            % Publish something without waiting for confirmation.
    async_publish_by_key/3,     % Publish something without waiting for confirmation by key.
    subscribe/1,                % Subscribe to a topic, returns {ok, ServerRef}
    subscribe/2,
    sync_request/2,
    sync_request/3,
    unsubscribe/1,              % Unsubscribe from a topic (takes ServerRef)
    create_queue/1,             % Create a queue
    delete_queue/1,             % Delete a queue
    unblogging_fun/1
]).


-define(DEFAULT_SYNC_REQUEST_TIMEOUT, 10000).


publish(Topic, Msg) -> jamq_publisher:publish(Topic, Msg).
publish(Topic, Msg, Timeout) -> jamq_publisher:publish(Topic, Msg, Timeout).
publish_by_key(Topic, Msg, Key) -> jamq_publisher:publish_by_key(Topic, Msg, Key).
publish_by_key(Topic, Msg, Key, Timeout) -> jamq_publisher:publish_by_key(Topic, Msg, Key, Timeout).

async_publish(Topic, Msg) -> jamq_publisher:async_publish(Topic, Msg).
async_publish_by_key(Topic, Msg, Key) -> jamq_publisher:async_publish_by_key(Topic, Msg, Key).


subscribe(Topic, Fun) ->
    jamq_subscriber_sup:start_link([{topic, Topic},{function, ?MODULE:unblogging_fun(Fun)}]).
subscribe([Option|_] = Options) when is_tuple(Option) ->
        FixedOptions = case proplists:get_value(function, Options) of
                undefined -> Options;
                Fun -> lists:keyreplace(function, 1, Options, {function, ?MODULE:unblogging_fun(Fun)})
        end,
        jamq_subscriber_sup:start_link(FixedOptions);
subscribe(Topic) when is_list(Topic); is_binary(Topic) ->
    jamq_subscriber_sup:start_link([{topic, Topic}]).

unsubscribe(ServerRef) -> jamq_subscriber_sup:stop(ServerRef).

create_queue({BrokerRole, QueueName}) when is_atom(BrokerRole), is_list(QueueName) ->
    jsk_async:complete(
        fun() ->
            Brokers = jamq_api:get_brokers(BrokerRole),
            lists:foreach(
                fun (B) ->
                    Channel = jamq_channel:channel(jamq_channel:name(BrokerRole, B)),
                    Queue = list_to_binary(QueueName),
                    jamq_api:declare_permanent_queue(Channel, Queue),
                    #'queue.bind_ok'{} =
                        lib_amqp:bind_queue(Channel, <<"jskit-bus">>, Queue, Queue)
                end, Brokers),
            ok
        end).

delete_queue({BrokerRole, Q}) when is_atom(BrokerRole), is_list(Q) ->
    jsk_async:complete(
        fun () ->
            Brokers = jamq_api:get_brokers(BrokerRole),
            lists:foreach(
                fun (B) ->
                    Channel = jamq_channel:channel(jamq_channel:name(BrokerRole, B)),
                    #'queue.delete_ok'{} = jamq_api:delete_queue(Channel, Q)
                end, Brokers),
            ok
        end).

% TODO: What is this??? Remove it
unblogging_fun(Fun) ->
    case erlang:fun_info(Fun, arity) of
        {_, 1} -> fun
                    ({blogged, _Lid, Msg}) -> Fun(Msg);
                    (Msg) -> Fun(Msg)
                  end;
        {_, 2} -> fun
                    ({blogged, _Lid, Msg}, Redel) -> Fun(Msg, Redel);
                    (Msg, Redel) -> Fun(Msg, Redel)
                  end;
        {_, 3} -> fun
                    ({blogged, _Lid, Msg}, Chan, Tag) -> Fun(Msg, Chan, Tag);
                    (Msg, Chan, Tag) -> Fun(Msg, Chan, Tag)
                  end;
        {_, 4} -> fun
                    ({blogged, _Lid, Msg}, Redel, Chan, Tag) -> Fun(Msg, Redel, Chan, Tag);
                    (Msg, Redel, Chan, Tag) -> Fun(Msg, Redel, Chan, Tag)
                  end
    end.

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
