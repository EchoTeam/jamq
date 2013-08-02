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
    new_config_migration0/0,
    new_config_migration1/0,
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

new_config_migration0() ->
    stream_server:reload_config(),
    catch reconfigure(),
    catch as_pipeline_sup:restart_children().

new_config_migration1() ->
    stream_server:reload_config(),
    io:format("Reconfiguring jamq_sup...~n"),
    catch reconfigure(),
    catch echo_view_config_server:reload(),
    io:format("Restarting as_pipeline_children...~n"),
    catch as_pipeline_sup:restart_children(),
    io:format("Reconfiguring coser_sup...~n"),
    catch coser_sup:reconfigure(),
    io:format("Restarting all jamq subscribers...~n"),
    catch jamq_subscriber:kill_all_subscribers(code_update, 2000).

get_brokers(Role) ->
    {ok, Config} = application:get_env(stream_server, amq_servers),
    case proplists:get_value(Role, Config, undefined) of
        Brokers when is_list(Brokers) -> Brokers;
        undefined ->
            lager:error("Invalid broker group name: ~p", [Role]),
            erlang:error({no_such_broker_group, Role})
    end.

