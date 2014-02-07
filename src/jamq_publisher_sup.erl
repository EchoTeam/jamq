%%%
%%% Copyright (c) 2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_publisher_sup).

-behaviour(supervisor).

-export([
    init/1,
    start_link/1,
    reconfigure/0,
    children_specs/1,
    format_status/0
]).

start_link(BrokerSpecs) ->
    lager:info("[start_link] Starting Echo AMQ publisher supervisor"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, BrokerSpecs).

init(BrokerSpecs) ->

    PublishersSpecs = lists:map(
        fun ({PublisherName, BrokersList}) when is_atom(PublisherName) ->
            {
                PublisherName,
                {jamq_publisher, start_link, [PublisherName, [jamq_channel:name(PublisherName, B) || B <- BrokersList]]},
                permanent, 10000, worker, [jamq_publisher]
            }
        end, BrokerSpecs),

    {ok, {{one_for_one, 10, 10}, PublishersSpecs}}.

children_specs({_, start_link, [BrokerSpecs]}) ->
    {ok, {_, Specs}} = init(BrokerSpecs),
    Specs.

reconfigure() ->
    {ok, BrokerSpecs} = application:get_env(jamq, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    superman:reconfigure_supervisor_tree(?MODULE, ChildSpecs).


format_status() ->
    L = supervisor:which_children(?MODULE),
    Strings = lists:map(fun ({_, P, _, _}) -> format_status(P) end, L),
    io:format("~.20s ~.20s ~.20s ~.20s~n~s~n", ["Name", "Pid", "MsgQLen", "MailBoxLen", string:join(Strings, "\n")]).

format_status(undefined) -> undefined;
format_status(P) when is_atom(P) -> format_status(whereis(P));
format_status(P) ->
    PL = gen_server:call(P, {status}),
    Role = io_lib:format("~p", [proplists:get_value(role, PL, undefined)]),
    MQLen = io_lib:format("~p", [proplists:get_value(queue_length, PL, undefined)]),
    MBLen = io_lib:format("~p", [element(2, erlang:process_info(P, message_queue_len))]),
    Pid = io_lib:format("~p", [P]),

    io_lib:format("~.20s ~.20s ~.20s ~.20s", [Role, Pid, MQLen, MBLen]).

