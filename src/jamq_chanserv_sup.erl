%%%
%%% Copyright (c) 2013 JackNyfe. All rights reserved.
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
    children_specs/1,
    format_status/0
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
    {ok, BrokerSpecs} = application:get_env(jamq, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    superman:reconfigure_supervisor(?MODULE, ChildSpecs).

start_new_channels() ->
    {ok, BrokerSpecs} = application:get_env(jamq, amq_servers),

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

format_status() ->
    Statuses = [jamq_channel:status(P) || {_, P, _, _} <- supervisor:which_children(?MODULE)],
    {Down, Up} = lists:partition(fun (S) -> undefined == proplists:get_value(connection, S, undefined) end, Statuses),

    {Inactive, Connecting} = lists:partition(fun (S) -> undefined == proplists:get_value(connection_establisher, S, undefined) end, Down),

    Format = fun (L) -> lists:map(fun (S) -> io_lib:format("~p~n", [proplists:get_value(role, S, undefined)]) end, L) end,

    io:format("Inactive (~p connections):~n~s~nConnecting (~p connections):~n~s~nUp (~p connections): ~n~s",
              [length(Inactive), Format(Inactive), length(Connecting), Format(Connecting), length(Up), Format(Up)]).


