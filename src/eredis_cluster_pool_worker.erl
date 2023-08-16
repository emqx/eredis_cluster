-module(eredis_cluster_pool_worker).
-behaviour(gen_server).
-behaviour(ecpool_worker).

%% ecpool.
-export([connect/1]).

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

-record(state, {conn, host, port, database, credentials}).

-define(RECONNECT_TIME, 2000).

connect(Args) ->
    start_link(Args).

is_connected(Pid) ->
    gen_server:call(Pid, is_connected).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    Hostname = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    DataBase = proplists:get_value(database, Args, 0),
    Credentials = proplists:get_value(credentials, Args, eredis:make_empty_credentials()),
    Options = proplists:get_value(options, Args, []),
    erlang:put(options, Options),
    process_flag(trap_exit, true),
    erlang:send(self(), reconnect),
    {ok, #state{conn = undefined,
                host = Hostname,
                port = Port,
                database = DataBase,
                credentials = Credentials}}.

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
                              credentials = Credentials} = State) ->
    Options = case erlang:get(options) of
        undefined -> [];
        Options0 -> Options0
    end,
    Conn = start_connection(Hostname, Port, DataBase, Credentials, Options),
    {noreply, State#state{conn = Conn}};

handle_info(reconnect, State) ->
    {noreply, State};

handle_info({'EXIT', Pid, _Reason}, #state{conn = Pid0} = State)
        when Pid0 =:= Pid; Pid0 =:= undefined ->
    erlang:send_after(?RECONNECT_TIME, self(), reconnect),
    {noreply, State#state{conn = undefined}};

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

censor_state(#state{credentials = Credentials0} = State) ->
    Credentials = eredis:redact_credentials(Credentials0),
    State#state{credentials = Credentials};
censor_state(State) ->
    State.

safe_query(Func, Conn, Commands) ->
    try eredis:Func(Conn, Commands)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

start_connection(Hostname, Port, DataBase, Credentials, Options) ->
    %% NOTE: `eredis:start_link/7` may raise exceptions if connect to redis failed,
    %%  so we will receive an 'EXIT' message.
    case eredis:start_link(Hostname, Port, DataBase, Credentials, no_reconnect, 5000, Options) of
        {ok,Connection} ->
            Connection;
        _ ->
            undefined
    end.
