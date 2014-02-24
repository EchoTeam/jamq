%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_supervisor).

-behaviour(supervisor).

-export([
    init/1,
    start_link/0,
    start_link/1,
    reconfigure/0,
    get_brokers/1, % temporarily function
    children_specs/1,
    restart_subscribers/0,
    force_restart_subscribers/0
]).

start_link() ->
    BrokerSpecs = case application:get_env(jamq, amq_servers) of
        {ok, B} -> B;
        undefined -> []
    end,
    start_link(BrokerSpecs).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    ChanservSup = {chanserv_sup, {jamq_chanserv_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_chanserv_sup]},

    PublishersSup = {publisher_sup, {jamq_publisher_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_publisher_sup]},

    SubscribersSup = {subscribers_sup, {jamq_subscriber_top_sup, start_link, []}, permanent, infinity, supervisor, [jamq_subscriber_top_sup]},

    {ok, {{one_for_all, 3, 10}, [ChanservSup, PublishersSup, SubscribersSup]}}.

children_specs({_, start_link, []}) ->
    {ok, BrokerSpecs} = application:get_env(jamq, amq_servers),
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs;
children_specs({_, start_link, [BrokerSpecs]}) ->
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs.

reconfigure() ->
    jamq_chanserv_sup:reconfigure(),
    restart_subscribers(),
    jamq_publisher_sup:reconfigure().

get_brokers(Role) ->
    {ok, Config} = application:get_env(jamq, amq_servers),
    case proplists:get_value(Role, Config, undefined) of
        Brokers when is_list(Brokers) -> Brokers;
        undefined ->
            lager:error("Invalid broker group name: ~p", [Role]),
            erlang:error({no_such_broker_group, Role})
    end.

restart_subscribers() ->
    L = supervisor:which_children(jamq_subscriber_top_sup),
    io:format("* Restarting subscribers... "),
    K = lists:foldl(
        fun
            ({_, undefined, _, _}, N) -> N;
            ({_, Ref, _, _}, N) ->
                try
                    ok = jamq_subscriber_sup:reconfigure(Ref),
                    N + 1
                catch
                    _:E ->
                        lager:error("Restart subscriber failed: ~p~nReason: ~p~nStacktrace: ~p", [Ref, E, erlang:get_stacktrace()]),
                        N
                end
        end, 0, L),
    io:format("~p/~p~n", [K, erlang:length(L)]).

force_restart_subscribers() ->
    L = supervisor:which_children(jamq_subscriber_top_sup),
    io:format("* Restarting subscribers... "),
    K = lists:foldl(
        fun
            ({_, undefined, _, _}, N) -> N;
            ({Ref, _, _, _}, N) ->
                try
                    ok = supervisor:terminate_child(jamq_subscriber_top_sup, Ref),
                    {ok, _} = supervisor:restart_child(jamq_subscriber_top_sup, Ref),
                    N + 1
                catch
                    _:E ->
                        lager:error("Restart subscriber failed: ~p~nReason: ~p~nStacktrace: ~p", [Ref, E, erlang:get_stacktrace()]),
                        N
                end
        end, 0, L),
    io:format("~p/~p~n", [K, erlang:length(L)]).
