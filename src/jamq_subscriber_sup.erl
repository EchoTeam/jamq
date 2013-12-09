
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_sup).

-behaviour(supervisor).


-export([
    start_link/1,
    init/1,
    stop/1,
    reconfigure/1
]).

start_link(Properties) ->
    try
        Owner = proplists:get_value(owner, Properties, proplists:get_value(default_owner, Properties, self())),
        {ok, _} = supervisor:start_link(?MODULE, {top, Owner, Properties})
    catch
        _:E ->
            lager:error("jamq_subscriber_sup start failed~nProperties: ~p~nError: ~p~nStacktrace: ~p", [Properties, E, erlang:get_stacktrace()]),
            {error, E}
    end.

stop(SupRef) ->
    C = supervisor:which_children(SupRef),
    {_, ChSup, _, _} = lists:keyfind(children, 1, C),

    lists:foreach(
        fun ({_, Child, _, _}) ->
            jamq_subscriber:unsubscribe(Child),
            catch supervisor:terminate_child(ChSup, Child)
        end, supervisor:which_children(ChSup)),

    lists:foreach(
        fun ({Name, _, _, _}) ->
            catch supervisor:terminate_child(SupRef, Name),
            catch supervisor:delete_child(SupRef, Name)
        end, supervisor:which_children(SupRef)),

    ok.

init({top, Owner, Properties}) ->
    {ok, {{one_for_one, 10, 10}, [
        {monitor,  {jamq_subscriber_mon, start_link, [Owner, self()]}, permanent, 10000, worker, [jamq_subscriber_mon]},
        {children, {supervisor, start_link, [?MODULE, {children, Properties}]}, permanent, infinity, supervisor, [?MODULE]}
    ]}};

init({children, Properties}) ->

    BrokerGroup = proplists:get_value(broker_group, Properties, undefined),

    ChanServs =
        case BrokerGroup of
            undefined ->
                ChanServ = proplists:get_value(broker, Properties, undefined),
                [ChanServ];
            _ ->
                Hosts = jamq_supervisor:get_brokers(BrokerGroup),
                [jamq_channel:name(BrokerGroup, H) || H <- Hosts]
        end,

   Specs = lists:map(
            fun (undefined) ->
                    erlang:error({missing, broker});
                (CS) ->
                    SubProps = lists:keystore(broker, 1, Properties, {broker, CS}),
                    {CS, {jamq_subscriber, start_link, [SubProps]}, transient, 10000, worker, [jamq_subscriber]}
            end, ChanServs),

    {ok, {{one_for_one, 10, 10}, Specs}}.

reconfigure(SupRef) ->
    C = supervisor:which_children(SupRef),
    {_, ChSup, _, _} = lists:keyfind(children, 1, C),
    {_, Monitor, _, _} = lists:keyfind(monitor, 1, C),
    Properties = jamq_subscriber_mon:get_properties(Monitor),
    {_, {_, Specs}} = init({children, Properties}),
    code_update_mod:reconfigure_supervisor(ChSup, Specs),
    ok.
