%% -------------------------------------------------------------------
%%
%% riak_kv_pb_listener: Listen for protocol buffer clients
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc entry point for TCP-based protocol buffers service

-module(riak_kv_pb_listener).
-behavior(gen_nb_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([sock_opts/0, new_connection/2, get_ssl_options/0]).
-record(state, {
          portnum :: integer(),
          ssl_opts :: list()
         }).

start_link() ->
    PortNum = app_helper:get_env(riak_kv, pb_port),
    IpAddr = app_helper:get_env(riak_kv, pb_ip),
	SslOpts = get_ssl_options(),
	gen_nb_server:start_link(?MODULE, IpAddr, PortNum, [PortNum, SslOpts]).

init([PortNum, SslOpts]) ->
	{ok, #state{portnum=PortNum, ssl_opts = SslOpts}}.

sock_opts() ->
    BackLog = app_helper:get_env(riak_kv, pb_backlog, 5),
    [binary, {packet, 4}, {reuseaddr, true}, {backlog, BackLog}].

handle_call(_Req, _From, State) -> 
    {reply, not_implemented, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

new_connection(Socket, State = #state{ssl_opts = SslOpts}) ->
	{ok, Pid} = riak_kv_pb_socket_sup:start_socket(SslOpts),
	% {ok, Pid} = riak_core_handoff_receiver:start_link(SslOpts), % FIXME Remover riak_core_handoff_receiver
	% {ok, Pid} = riak_kv_pb_socket_sup:start_socket(),
    ok = gen_tcp:controlling_process(Socket, Pid),
    ok = riak_kv_pb_socket:set_socket(Pid, Socket),
    {ok, State}.

get_ssl_options() ->
    case app_helper:get_env(riak_kv, ssl, []) of
        [] ->
            [];
        Props ->
            try
                %% We'll check if the file(s) exist but won't check
                %% file contents' sanity.
                ZZ = [{_, {ok, _}} = {ToCheck, file:read_file(Path)} ||
                         ToCheck <- [certfile, keyfile, cacertfile, dhfile],
                         Path <- [proplists:get_value(ToCheck, Props)],
                         Path /= undefined],
                spawn(fun() -> self() ! ZZ end), % Avoid term...never used err
                %% Props are OK
                Props
            catch
                error:{badmatch, {FailProp, BadMat}} ->
                    error_logger:error_msg("riak_kv ssl "
                                           "config error: property ~p: ~p.  "
                                           "Disabling Protocol Buffers SSL\n",
                                           [FailProp, BadMat]),
                    [];
                X:Y ->
                    error_logger:error_msg("riak_kv ssl "
                                           "failure {~p, ~p} processing config "
                                           "~p.  Disabling Protocol Buffers SSL\n",
                                           [X, Y, Props]),
                    []
            end
    end.