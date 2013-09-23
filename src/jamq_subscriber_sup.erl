
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_sup).

-behaviour(supervisor).


-export([
    start_link/2,
    init/1,
    stop/1
]).


start_link(Ref, Properties) ->
    try
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

        Owner = proplists:get_value(owner, Properties, proplists:get_value(default_owner, Properties, self())),

        {ok, TopSup} = supervisor:start_link(?MODULE, {top, Owner, Ref}),

        {ok, Sup} = supervisor:start_child(TopSup, {children, {supervisor, start_link, [?MODULE, children]}, permanent, infinity, supervisor, [?MODULE]}),

        lager:info("Starting subscribers for BrokerGroup ~p(~p), chan_servers: ~p", [BrokerGroup, proplists:get_value(broker, Properties, undefined), ChanServs]),

        lists:foreach(
            fun (undefined) ->
                    erlang:error({missing, broker});
                (CS) ->
                    SubProps = lists:keystore(broker, 1, Properties, {broker, CS}),
                    {ok, _} = supervisor:start_child(Sup, [SubProps])
            end, ChanServs),

        {ok, TopSup}
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

    % Terminate me with supervisor:terminate_child/2
    %erlang:exit(SupRef, kill),
    ok.

init({top, Owner, Ref}) ->
    {ok, {{one_for_one, 0, 1}, [{subscriber_monitor, {jamq_subscriber_mon, start_link, [Owner, self(), Ref]}, permanent, 10000, worker, [jamq_subscriber_mon]}]}};
init(children) ->
    {ok, {{simple_one_for_one, 10, 10}, [{jamq_subscriber, {jamq_subscriber, start_link, []}, transient, 10000, worker, [jamq_subscriber]}]}}.


