-module(eredis_cluster_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API.
-export([start_link/1]).
-export([query/2, is_connected/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([format_status/1]).
-export([format_status/2]).

-record(state, {conn, host, port, database, password}).

-define(RECONNECT_MAX, 4096000). %% 4096s ≈ 1.13 hours

is_connected(Pid) ->
    gen_server:call(Pid, is_connected).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    Hostname = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    DataBase = proplists:get_value(database, Args, 0),
    Password = proplists:get_value(password, Args, ""),
    Options = proplists:get_value(options, Args, []),
    erlang:put(options, Options),
    process_flag(trap_exit, true),
    erlang:send(self(), reconnect),
    {ok, #state{conn = undefined,
                host = Hostname,
                port = Port,
                database = DataBase,
                password = Password}}.

query(Worker, Commands) ->
    gen_server:call(Worker, {'query', Commands}).

handle_call({'query', _}, _From, #state{conn = undefined} = State) ->
    {reply, {error, no_connection}, State};
handle_call({'query', [[X|_]|_] = Commands}, _From, #state{conn = Conn} = State)
    when is_list(X); is_binary(X) ->
    {reply, safe_query(qp, Conn, Commands), State};
handle_call({'query', Command}, _From, #state{conn = Conn} = State) ->
    {reply, safe_query(q, Conn, Command), State};
handle_call(is_connected, _From, #state{conn = Conn}= State) ->
    {reply, Conn =/= undefined andalso is_process_alive(Conn), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(reconnect, #state{conn = undefined,
                              host = Hostname,
                              port = Port,
                              database = DataBase,
                              password = Password} = State) ->
    Options = case erlang:get(options) of
        undefined -> [];
        Options0 -> Options0
    end,
    Attempts = get_reconnect_attempts(),
    case start_connection(Hostname, Port, DataBase, Password, Options) of
        undefined ->
            %% Connection failed (e.g. SSL handshake error during cert rotation).
            %% Use exponential backoff to avoid flooding the server with zombie
            %% TCP connections that accumulate behind NLB/proxy layers.
            Delay = backoff_delay(Attempts),
            erlang:send_after(Delay, self(), reconnect),
            put_reconnect_attempts(Attempts + 1),
            {noreply, State};
        Conn ->
            put_reconnect_attempts(0),
            {noreply, State#state{conn = Conn}}
    end;

handle_info(reconnect, State) ->
    {noreply, State};

handle_info({'EXIT', Pid, _Reason}, #state{conn = Pid0} = State)
        when Pid0 =:= Pid; Pid0 =:= undefined ->
    case Pid0 of
        undefined ->
            %% EXIT from a failed start_connection attempt (gen_server:start_link
            %% links before init runs). A reconnect is already scheduled with
            %% backoff by the reconnect handler — do NOT schedule another one
            %% to avoid bypassing the backoff delay.
            {noreply, State};
        _ ->
            %% EXIT from an established connection that went down.
            %% Reset backoff and schedule immediate reconnect.
            erlang:send_after(backoff_delay(0), self(), reconnect),
            put_reconnect_attempts(0),
            {noreply, State#state{conn = undefined}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=undefined}) ->
    ok;
terminate(_Reason, #state{conn=Conn}) ->
    ok = eredis:stop(Conn),
    ok.

code_change(_, State, _Extra) ->
    {ok, State}.

format_status(Status = #{state := State}) ->
    Status#{state => censor_state(State)}.

%% TODO
%% This is deprecated since OTP-25 in favor of `format_status/1`. Remove once
%% OTP-25 becomes minimum supported OTP version.
format_status(_Opt, [_PDict, State]) ->
    [{data, [{"State", censor_state(State)}]}].

censor_state(#state{} = State) ->
    State#state{password = "******"};
censor_state(State) ->
    State.
    
safe_query(Func, Conn, Commands) ->
    try eredis:Func(Conn, Commands)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

start_connection(Hostname, Port, DataBase, Password, Options) ->
    %% NOTE: `eredis:start_link/7` may raise exceptions if connect to redis failed,
    %%  so we will receive an 'EXIT' message.
    case eredis:start_link(Hostname, Port, DataBase, Password, no_reconnect, 5000, Options) of
        {ok,Connection} ->
            Connection;
        _ ->
            undefined
    end.

%% Exponential backoff with each interval used twice (except the first):
%%   2s, 4s, 4s, 8s, 8s, 16s, 16s, 32s, 32s, ... up to ?RECONNECT_MAX (default 4096s).
%% Formula: min(2^((N+1) div 2 + 1), MAX) seconds, where N = Attempts (0-based).
backoff_delay(N) ->
    Exp = (N + 1) div 2 + 1,
    min((1 bsl Exp) * 1000, ?RECONNECT_MAX).

get_reconnect_attempts() ->
    case erlang:get(reconnect_attempts) of
        undefined -> 0;
        N -> N
    end.

put_reconnect_attempts(N) ->
    erlang:put(reconnect_attempts, N).
