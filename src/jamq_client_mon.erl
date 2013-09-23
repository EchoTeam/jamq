
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_client_mon).

-behavior(gen_server).

-export([
    start_link/1,
    stop/1
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
    sub_ref = undefined,
    options = undefined,
    man_mon = undefined
    }).

start_link(Options) ->
    case lists:keytake(server_name, 1, Options) of
        {value, {server_name, {A, _} = ServerName}, Options2} when is_atom(A) ->
            gen_server:start_link(ServerName, ?MODULE, [Options2], []);
        false ->
            gen_server:start_link(?MODULE, [Options], [])
    end.

stop(Ref) ->
    gen_server:call(Ref, stop).

init([Options]) ->
    Mon = erlang:monitor(process, jamq_subscriber_man),
    {ok, start_sub(#state{options = Options, man_mon = Mon})}.

handle_call(stop, _From, State = #state{sub_ref = Ref}) ->
    catch jamq:stop_subscriber(Ref),
    {stop, normal, ok, State#state{sub_ref = undefined}};

handle_call(Req, _From, State) ->
    lager:error("Unhandled call ~p", [Req]),
    {noreply, State}.

handle_cast(Req, State) ->
    lager:error("Unhandled cast ~p", [Req]),
    {noreply, State}.

handle_info({'DOWN', Ref, _, _, _}, State = #state{man_mon = Ref}) ->
    {stop, jamq_stop, State#state{man_mon = undefined}};

handle_info(Req, State = #state{}) ->
    lager:error("Unhandled info ~p", [Req]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("~p terminate(~p)", [self(), Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% INTERNAL FUNCTIONS

start_sub(State = #state{options = Options}) ->
    {ok, Ref} = jamq:start_subscriber([{owner, self()}|Options]),
    State#state{sub_ref = Ref}.



