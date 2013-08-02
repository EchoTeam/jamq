%%%
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_chanserv_sup).

-behaviour(supervisor).

-export([
    init/1,
    start_link/1,
    reconfigure/0,
    start_new_channels/0,
    children_specs/1
]).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ chanserv supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    ChannelsSpecs =
        [spec(BrokerGroup, BrokerHostName) || {BrokerGroup, BrokerHosts} <-  BrokerSpecs, BrokerHostName <- BrokerHosts],

    {ok, {{one_for_one, 10, 10}, ChannelsSpecs}}.

children_specs({_, start_link, [BrokerSpecs]}) ->
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs.

reconfigure() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    code_update_mod:reconfigure_supervisor(?MODULE, ChildSpecs).

start_new_channels() ->
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),

    NewSpecs =
        [spec(BrokerGroup, BrokerHostName) || {BrokerGroup, BrokerHosts} <-  BrokerSpecs, BrokerHostName <- BrokerHosts],

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

spec(Group, Broker) ->
    {
        process_name([erlang:atom_to_list(Group), Broker]),
        {jamq_channel, start_link, [jamq_channel:name(Group, Broker), Broker]},
        permanent, 10000, worker, [jamq_channel]
    }.

process_name(List) ->
    list_to_atom(string:join(List, "_")).


