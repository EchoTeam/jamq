
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_man).

-behavior(gen_server).

-export([
    start_link/0,
    start_subscriber/1,
    stop_subscriber/1,
    child_restarted/2,
    remove_subscriber/2
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
    }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_subscriber(Options) ->
    gen_server:call(?MODULE, {start, [{default_owner, self()}|Options]}).

stop_subscriber(Ref) ->
    gen_server:call(?MODULE, {stop, Ref}).

child_restarted(Ref, Pid) ->
    gen_server:cast(?MODULE, {child_restarted, Ref, Pid}).

remove_subscriber(Ref, Pid) ->
    gen_server:cast(?MODULE, {remove_subscriber, Ref, Pid}).

init([]) ->
    ets:new(jamq_subscribers, [set, named_table, protected]),
    {ok, #state{}}.

handle_call({start, Options}, _From , State) ->
    try
        {Ref, Options2} =
            case lists:keytake(server_name, 1, Options) of
                {value, {server_name, {A, ServerName}}, O} when is_atom(A) ->
                    {ServerName, O};
                false ->
                    {erlang:make_ref(), Options}
            end,

        case ets:lookup(jamq_subscribers, Ref) of
            [{_, OldPid}] -> throw({error, {already_started, OldPid}});
            [] -> ok
        end,

        {ok, Pid} = jamq_subscriber_top_sup:start_subscriber(Ref, Options2),

        lager:debug("Subscriber ~p: started - ~p", [Ref, Pid]),

        true = ets:insert_new(jamq_subscribers, {Ref, Pid}),

        {reply, {ok, Ref}, State}

    catch
        throw:{error, E} ->
            lager:error("Start subscriber failed~n~p~nReason: ~p", [Options, E]),
            {reply, {error, E}, State};
        _:E ->
            lager:error("Start subscriber failed~n~p~nReason: ~p~nStacktrace: ~p", [Options, E, erlang:get_stacktrace()]),
            {reply, {error, E}, State}
    end;

handle_call({stop, Ref}, _From, State) ->
    case ets:lookup(jamq_subscribers, Ref) of
        [{Ref, Pid}] ->
            ets:delete(jamq_subscribers, Ref),
            Res = jamq_subscriber_top_sup:stop_subscriber(Pid),
            lager:info("Subscriber ~p: stopped - ~p", [Ref, Pid]),
            {reply, Res, State};
        [] ->
            lager:error("Attempt to stop wrong subscriber: ~p", [Ref]),
            {reply, {error, not_found}, State}
    end;

handle_call(Req, _From, State) ->
    lager:error("Unhandled call ~p", [Req]),
    {noreply, State}.

handle_cast({child_restarted, Ref, Pid}, State) ->
    case ets:lookup(jamq_subscribers, Ref) of
        [{Ref,_ }] ->
            ets:insert(jamq_subscribers, {Ref, Pid}),
            lager:info("Subscriber ~p: (re)started - ~p", [Ref, Pid]);
        [] ->
            jamq_subscriber_top_sup:stop_subscriber(Pid),
            lager:info("Subscriber ~p: restarted and stopped - ~p", [Ref, Pid])
    end,
    {noreply, State};

handle_cast({remove_subscriber, Ref, Pid}, State) ->
    jamq_subscriber_top_sup:stop_subscriber(Pid),
    ets:delete_object(jamq_subscribers, {Ref, Pid}),
    lager:info("Subscriber ~p: removed - ~p", [Ref, Pid]),
    {noreply, State};

handle_cast(Req, State) ->
    lager:error("Unhandled cast ~p", [Req]),
    {noreply, State}.

handle_info(Req, State = #state{}) ->
    lager:error("Unhandled info ~p", [Req]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("~p terminate(~p)", [self(), Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% INTERNAL FUNCTIONS



