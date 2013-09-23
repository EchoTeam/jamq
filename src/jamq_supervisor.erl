%%%
%%% Copyright (c) 2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_supervisor).

-behaviour(supervisor).

-include("jamq.hrl").

-export([
    init/1,
    start_link/0,
    start_link/1,
    reconfigure/0,
    get_brokers/1, % temporarily function
    children_specs/1,
    restart_subscribers/0
]).

start_link() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    start_link(BrokerSpecs).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    ChanservSup = {chanserv_sup, {jamq_chanserv_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_chanserv_sup]},

    PublishersSup = {publisher_sup, {jamq_publisher_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_publisher_sup]},

    SubscribersSup = {subscribers_sup, {jamq_subscriber_top_sup, start_link, []}, permanent, infinity, supervisor, [jamq_subscriber_top_sup]},

    SubsManager = {subscribers_man, {jamq_subscriber_man, start_link, []}, permanent, 10000, worker, [jamq_subscriber_man]},

    {ok, {{one_for_all, 3, 10}, [ChanservSup, PublishersSup, SubscribersSup, SubsManager]}}.

children_specs({_, start_link, []}) ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs;
children_specs({_, start_link, [BrokerSpecs]}) ->
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs.

reconfigure() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    code_update_mod:reconfigure_supervisor_tree(?MODULE, ChildSpecs).

get_brokers(Role) ->
    {ok, Config} = application:get_env(stream_server, amq_servers),
    case proplists:get_value(Role, Config, undefined) of
        Brokers when is_list(Brokers) -> Brokers;
        undefined ->
            lager:error("Invalid broker group name: ~p", [Role]),
            erlang:error({no_such_broker_group, Role})
    end.

restart_subscribers() ->
    L = supervisor:which_children(jamq_subscriber_top_sup),

    lists:foldl(
        fun ({_, P, _, _}, N) ->
            % 1) It is simple_one_for_one restart strategy, so we can't use supervisor:restart_child
            % 2) If we kill processes too often supervisor will crash because of reached_max_restart_intensity
            ((N rem (?JAMQ_SUBSCRIBERS_MAX_R-1)) == 0) andalso timer:sleep((?JAMQ_SUBSCRIBERS_MAX_T+1) * 1000),
            erlang:exit(P, kill),
            N + 1
        end, 1, L).
