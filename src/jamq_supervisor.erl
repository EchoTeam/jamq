%%%
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
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
    start_new_channels/0,
    get_brokers/1, % temporarily function
    new_config_migration0/0,
    new_config_migration1/0
]).

start_link() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    start_link(BrokerSpecs).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    ChannelsSpecs =
        [{
            process_name(["jamq_connection", erlang:atom_to_list(BrokerGroup), BrokerHostName]),
            {jamq_channel, start_link, [jamq_channel:name(BrokerGroup, BrokerHostName), BrokerHostName]},
            permanent, 10000, worker, [jamq_channel]
         } || {BrokerGroup, BrokerHosts} <-  BrokerSpecs, BrokerHostName <- BrokerHosts],

    PublishersSpecs = lists:map(
        fun ({PublisherName, BrokersList}) when is_atom(PublisherName) ->
            {
                process_name(["jamq_publisher", erlang:atom_to_list(PublisherName)]),
                {jamq_publisher, start_link, [PublisherName, [jamq_channel:name(PublisherName, B) || B <- BrokersList]]},
                permanent, 10000, worker, [jamq_publisher]
            }
        end, BrokerSpecs),

    {ok, {{one_for_one, 10, 10}, ChannelsSpecs ++ PublishersSpecs}}.

reconfigure() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    code_update_mod:reconfigure_supervisor(?MODULE, ChildSpecs).

start_new_channels() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),

    NewSpecs =
        [{
            process_name(["jamq_connection", erlang:atom_to_list(BrokerGroup), BrokerHostName]),
            {jamq_channel, start_link, [jamq_channel:name(BrokerGroup, BrokerHostName), BrokerHostName]},
            permanent, 10000, worker, [jamq_channel]
         } || {BrokerGroup, BrokerHosts} <-  BrokerSpecs, BrokerHostName <- BrokerHosts],

    lists:foreach(
        fun({Id, _, _, _, _, _} = S) ->
            io:format("Starting ~p... ", [Id]),
            case supervisor:start_child(?MODULE, S) of
                {ok, _} -> io:format("ok~n");
                {error, {already_started, _}} -> io:format("skip~n");
                {error, Error} -> io:format("error (~1000000p)~n", [Error])
            end
        end, NewSpecs),

    io:format("Channels restart finished~n").


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

process_name(List) ->
    list_to_atom(string:join(List, "_")).

get_brokers(Role) ->
    {ok, Config} = application:get_env(stream_server, amq_servers),
    case proplists:get_value(Role, Config, undefined) of
        Brokers when is_list(Brokers) -> Brokers;
        undefined ->
            lager:error("Invalid broker group name: ~p", [Role]),
            erlang:error({no_such_broker_group, Role})
    end.

