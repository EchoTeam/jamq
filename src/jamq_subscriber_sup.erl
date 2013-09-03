
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_sup).

-behaviour(supervisor).


-export([
    start_link/1,
    init/1,
    stop/1
]).


start_link(Properties) ->
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

        {ok, Sup} =
            case lists:keyfind(server_name, 1, Properties) of
                {server_name, {A, _} = ServerName} when is_atom(A) ->
                    supervisor:start_link(ServerName, ?MODULE, []);
                false ->
                    supervisor:start_link(?MODULE, [])
            end,

        lager:info("Starting subscribers for BrokerGroup ~p(~p), chan_servers: ~p", [BrokerGroup, proplists:get_value(broker, Properties, undefined), ChanServs]),

        lists:foreach(
            fun (undefined) ->
                    erlang:error({missing, broker});
                (CS) ->
                    SubProps = lists:keystore(broker, 1, Properties, {broker, CS}),
                    {ok, _} = supervisor:start_child(Sup, [SubProps])
            end, ChanServs),

        {ok, Sup}
    catch
        _:E ->
            lager:error("jamq_subscriber_sup start failed~nProperties: ~p~nError: ~p~nStacktrace: ~p", [Properties, E, erlang:get_stacktrace()]),
            {error, E}
    end.

stop(SupRef) ->
    lists:foreach(
        fun ({Name, Child, _, _}) ->
            jamq_subscriber:unsubscribe(Child),
            catch supervisor:terminate_child(SupRef, Child),
            catch supervisor:delete_child(SupRef, Name)
        end, supervisor:which_children(SupRef)),

    % Terminate me with supervisor:terminate_child/2
    %erlang:exit(SupRef, kill),
    ok.

init(_) ->
    {ok, {{simple_one_for_one, 10, 10}, [{jamq_subscriber, {jamq_subscriber, start_link, []}, transient, 10000, worker, [jamq_subscriber]}]}}.


