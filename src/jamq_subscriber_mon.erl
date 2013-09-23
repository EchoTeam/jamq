
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
    owner     = undefined,
    owner_mon = undefined,
    sup_pid   = undefined,
    ref       = undefined
    }).

start_link(Owner, SupPid, Ref) ->
    gen_server:start_link(?MODULE, [Owner, SupPid, Ref], []).

init([Owner, SupPid, Ref]) ->
    Mon = erlang:monitor(process, Owner),
    jamq_subscriber_man:child_restarted(Ref, SupPid),
    {ok, #state{owner = Owner, owner_mon = Mon, sup_pid = SupPid, ref = Ref}}.

handle_call(Req, _From, State) ->
    lager:error("Unhandled call ~p", [Req]),
    {noreply, State}.

handle_cast(Req, State) ->
    lager:error("Unhandled cast ~p", [Req]),
    {noreply, State}.

handle_info({'DOWN', MonRef, process, _, _}, State = #state{owner_mon = MonRef, sup_pid = Sup, ref = Ref}) ->
    jamq_subscriber_man:remove_subscriber(Ref, Sup),
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


