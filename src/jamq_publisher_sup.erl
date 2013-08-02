%%%
%%% Copyright (c) 2012 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(jamq_publisher_sup).

-behaviour(supervisor).

-export([
    init/1,
    start_link/1,
    reconfigure/0,
    children_specs/1
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
    {ok, BrokerSpecs} = application:get_env(stream_server, amq_servers),
    {ok, { _, ChildSpecs }} = init(BrokerSpecs),
    code_update_mod:reconfigure_supervisor_tree(?MODULE, ChildSpecs).


