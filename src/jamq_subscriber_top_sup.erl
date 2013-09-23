
%% vim: set ts=4 sts=4 sw=4 et:

-module(jamq_subscriber_top_sup).

-behaviour(supervisor).

-include("jamq.hrl").

-export([
    start_link/0,
    start_subscriber/2,
    stop_subscriber/1,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_subscriber(Ref, Options) ->
    supervisor:start_child(?MODULE, [Ref, Options]).

stop_subscriber(Ref) ->
    catch jamq_subscriber_sup:stop(Ref),
    supervisor:terminate_child(?MODULE, Ref).

init(_) ->
    {ok, {{simple_one_for_one, ?JAMQ_SUBSCRIBERS_MAX_R, ?JAMQ_SUBSCRIBERS_MAX_T}, [{jamq_subscriber_sup, {jamq_subscriber_sup, start_link, []}, permanent, infinity, supervisor, [jamq_subscriber_sup]}]}}.


