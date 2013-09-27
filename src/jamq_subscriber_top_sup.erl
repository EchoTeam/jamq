
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_top_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    start_subscriber/1,
    stop_subscriber/1,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_subscriber(Options) ->
    Ref = erlang:make_ref(),
    case supervisor:start_child(?MODULE, {Ref, {jamq_subscriber_sup, start_link, [Options]}, permanent, infinity, supervisor, [jamq_subscriber_sup]}) of
        {ok, _} -> {ok, Ref};
        Error -> Error
    end.

stop_subscriber(Ref) when is_reference(Ref) ->
    case lists:keyfind(Ref, 1, supervisor:which_children(?MODULE)) of
        {Ref, Pid, _, _} when is_pid(Pid) ->
            catch jamq_subscriber_sup:stop(Pid),
            catch supervisor:terminate_child(?MODULE, Ref),
            ok = supervisor:delete_child(?MODULE, Ref);
        {Ref, undefined, _, _} ->
            catch supervisor:terminate_child(?MODULE, Ref),
            ok = supervisor:delete_child(?MODULE, Ref);
        _ -> ok
    end;

stop_subscriber(Pid) when is_pid(Pid) ->
    catch jamq_subscriber_sup:stop(Pid),
    case lists:keyfind(Pid, 2, supervisor:which_children(?MODULE)) of
        {Ref, Pid, _, _} when is_pid(Pid) ->
            catch supervisor:terminate_child(?MODULE, Ref),
            ok = supervisor:delete_child(?MODULE, Ref);
        _ -> ok
    end.

init(_) ->
    {ok, {{one_for_one, 10, 10}, []}}.


