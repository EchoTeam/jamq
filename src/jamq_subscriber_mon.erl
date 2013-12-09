
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_mon).

-behavior(gen_server).

-export([
    start_link/3
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    owner_mon = undefined,
    sup_pid   = undefined,
    properties = undefined
    }).

start_link(Owner, SupPid, Properties) ->
    gen_server:start_link(?MODULE, [Owner, SupPid, Properties], []).

init([Owner, SupPid, Properties]) ->
    Mon = erlang:monitor(process, Owner),
    {ok, #state{owner_mon = Mon, sup_pid = SupPid, properties = Properties}}.

handle_call(get_properties, _From, State = #state{}) ->
    State#state.properties;
handle_call(Req, _From, State) ->
    lager:error("Unhandled call ~p", [Req]),
    {noreply, State}.

handle_cast(Req, State) ->
    lager:error("Unhandled cast ~p", [Req]),
    {noreply, State}.

handle_info({'DOWN', MonRef, process, _, _}, State = #state{owner_mon = MonRef, sup_pid = Sup}) ->
    spawn(fun () -> jamq_subscriber_top_sup:stop_subscriber(Sup) end),
    {noreply, State};

handle_info(Req, State = #state{}) ->
    lager:error("Unhandled info ~p", [Req]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("~p terminate(~p)", [self(), Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% INTERNAL FUNCTIONS


