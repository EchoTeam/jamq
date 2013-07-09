%%
%% AMQ channel keeper server.
%%
%% FUNCTIONS
%%    channel/0    Obtain a garbage-collected channel for custom purposes.
%% vim: set ts=4 sts=4 sw=4 et:
%%
-module(jamq_channel).

-behavior(gen_server).

-export([
    channel/1,
    channel_caller_mon/2,
    channel_caller_mon_loop/2,
    connection_caller_mon_loop/2,
    connection/1,
    name/2,
    start_link/2,
    status/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

status(ChanServer) when is_atom(ChanServer) -> gen_server:call(ChanServer, {status}).

name(BrokerGroup, Host) when is_atom(BrokerGroup), is_list(Host) ->
    list_to_atom("amqp_channel_server_" ++ erlang:atom_to_list(BrokerGroup) ++ "_" ++ Host).

channel(ChanServer) when is_atom(ChanServer) ->
    Channel = case gen_server:call(ChanServer, {get_channel}) of
        {ok, C} -> C;
        Error ->
            % !!! This sleep blocks jamq clients for two seconds in case of problems with amq connection.
            % (it blocks publisher, and then publisher blocks client).
            % Probably we should get rid of this.
            % TODO: Get rid of sleep
            timer:sleep(2000),
            throw(Error)
    end,
    CallerPid = self(),
    spawn_link(?MODULE, channel_caller_mon, [Channel, CallerPid]),
    Channel.

start_chan_serv_mon(Conn, Caller) ->
    erlang:link(Conn),
    CallerRef = erlang:monitor(process, Caller),
    ?MODULE:connection_caller_mon_loop(CallerRef, Conn).

connection_caller_mon_loop(CallerRef, Conn) ->
    receive
        {'DOWN', CallerRef, process, _, _} -> lib_amqp:close_connection(Conn)
    after 1000 ->
        ?MODULE:connection_caller_mon_loop(CallerRef, Conn)
    end.

channel_caller_mon(Channel, CallerPid) ->
    %% Avoid being killed on link failure, but still automatically
    %% kill CallerPid if we die for reason like code purge.
    process_flag(trap_exit, true),
    _ChannelRef = erlang:monitor(process, Channel),
    _CallerRef = erlang:monitor(process, CallerPid),
    ?MODULE:channel_caller_mon_loop(Channel, CallerPid).

channel_caller_mon_loop(Channel, CallerPid) ->
    receive
        {'DOWN', _ChannelRef, process, Channel, _} ->
            ok;
        {'DOWN', _CallerRef, process, CallerPid, _} ->
            lib_amqp:close_channel(Channel)
        after 1000 -> ?MODULE:channel_caller_mon_loop(Channel, CallerPid)
    end.

connection(Role) when is_atom(Role) ->
    case gen_server:call(Role, {get_connection}) of
        {_Ref, Pid} -> Pid;
        undefined -> undefined
    end.

-record(state, { role, hostname, connection, conn_establisher }).

start_link(Name, HostName) when is_atom(Name), is_list(HostName) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, HostName], []).

init([Name, HostName]) when is_atom(Name), is_list(HostName) ->
    {ok, asynchronous_connect(#state{role = erlang:list_to_atom(HostName), hostname = HostName}) }.

handle_call({get_connection}, _From, State) ->
    {reply, State#state.connection, State};
handle_call({get_channel}, _From, #state{connection = undefined} = State) ->
    {reply, {error, no_amqp_channel}, State};
handle_call({get_channel}, _From, #state{connection = {_, Conn}} = State) ->
    ChannelReply = try {ok, lib_amqp:start_channel(Conn)} catch
        C:R -> {error, {amqp_start_channel, {C,R}}}
    end,
    {reply, ChannelReply, State};
handle_call({status}, _From, State) ->
    {reply, [
        {role, State#state.role},
        {connection, State#state.connection},
        {connection_alive, case State#state.connection of
                undefined -> false;
                {_, Pid} -> is_process_alive(Pid)
                end},
        {connection_establisher, State#state.conn_establisher}
        ], State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({connection_established, E, Conn}, #state{conn_establisher = E, hostname = HostName} = State) ->
    lager:info("Established AMQ connection ~p to ~p", [Conn, HostName]),
    MRef = erlang:monitor(process, Conn),
    Self = self(),
    erlang:spawn(fun () -> start_chan_serv_mon(Conn, Self) end),
    {noreply, State#state{connection = {MRef, Conn}, conn_establisher = undefined}};
handle_info({connection_established, _, Conn}, State) ->
    % Closing unwanted open connection.
    spawn(fun() -> lib_amqp:close_connection(Conn) end),
    {noreply, State};
handle_info({connection_failed, E, Reason}, #state{conn_establisher = E} = State) ->
    lager:info("AMQP connection (~p) failed: ~p", [State#state.role, Reason]),
    {noreply, asynchronous_connect(State)};
handle_info({'DOWN', MRef, process, Pid, Info}, #state{connection = {MRef, Pid}} = State) ->
    lager:info("AMQP connection (~p) went down: ~p", [State#state.role, Info]),
    {noreply, asynchronous_connect(State#state{connection = undefined})};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% INTERNAL FUNCTIONS

asynchronous_connect(#state{hostname = BrokerHostname} = State) ->
    Self = self(),
    Establisher = jsk_async:apply_after(
        fun() ->
            Connection = lib_amqp:start_connection(BrokerHostname),
            Channel = lib_amqp:start_channel(Connection),
            jamq_api:declare_permanent_exchange(Channel,
                <<"jskit-bus">>, <<"topic">>),
            jamq_api:declare_permanent_exchange(Channel,
                <<"echo-live">>, <<"fanout">>),
            jamq_api:declare_permanent_exchange(Channel,
                <<"echo-e3-live">>, <<"fanout">>),
            lib_amqp:close_channel(Channel),
            Connection
        end,
        fun
          (Establisher, {ok, Conn}) ->
            Self ! {connection_established, Establisher, Conn};
          (Establisher, {error, Reason}) ->
            timer:sleep(15000),
            Self ! {connection_failed, Establisher, Reason}
        end),
    State#state{conn_establisher = Establisher}.

