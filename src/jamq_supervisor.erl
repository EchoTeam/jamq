%%%
%%% Copyright (c) 2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_supervisor).

-behaviour(supervisor).

-export([
    init/1,
    start_link/0,
    start_link/1,
    reconfigure/0,
    get_brokers/1, % temporarily function
    children_specs/1
]).

start_link() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    start_link(BrokerSpecs).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    ChanservSup = {chanserv_sup, {jamq_chanserv_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_chanserv_sup]},

    PublishersSpecs = {publisher_sup, {jamq_publisher_sup, start_link, [BrokerSpecs]}, permanent, infinity, supervisor, [jamq_publisher_sup]},

    {ok, {{one_for_one, 10, 10}, [ChanservSup, PublishersSpecs]}}.

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

