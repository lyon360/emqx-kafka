%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-include("emqx.hrl").
% -include_lib("brod/include/brod_int.hrl").
-include_lib("erlkaf/include/erlkaf.hrl").

-define(APP, emqx_plugin_kafka).

-define(ETS_TABLE_CACHE, ets_table_cache).  % cache key

-define(KAFKA_PRODUCER_CLIENT_LISTS, <<"KAFKA_CLIENT_LIST_CACHE">>).  % cache key

-define(MQTT_KAFKA_MAPPING_LIST, <<"MQTT_KAFKA_MAPPING_LIST">>).  % cache key

-define(DEFAULT_TOPIC_MAPPING, <<"DEFAULT_TOPIC_MAPPING">>).  % cache key

-define(CONNECTION_TOPIC_MAPPING, <<"CONNECTION_TOPIC_MAPPING">>).  % cache key

-define(MAPPING_CACHE_REFRESH_TIMER, <<"MAPPING_CACHE_REFRESH_TIMER">>).  % cache key


-export([ load/1
        , unload/0
        , mapping_cache_interval_refresh/0
        , delivery_report/2
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connect/3
        , on_client_connack/4
        , on_client_connected/3
        , on_client_disconnected/4
        , on_client_authenticate/3
        , on_client_check_acl/5
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_resumed/3
        , on_session_discarded/3
        , on_session_takeovered/3
        , on_session_terminated/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        , on_message_dropped/4
        ]).

%% Called when the plugin application start
load(Env) ->
    erlkaf:start(),
    ets:new(?ETS_TABLE_CACHE, [named_table, public]),
    mapping_cache_interval_refresh(),

    emqx:hook('client.connect',      {?MODULE, on_client_connect, [Env]}),
    emqx:hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
    emqx:hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
    emqx:hook('client.check_acl',    {?MODULE, on_client_check_acl, [Env]}),
    emqx:hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
    emqx:hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
    emqx:hook('session.created',     {?MODULE, on_session_created, [Env]}),
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('session.resumed',     {?MODULE, on_session_resumed, [Env]}),
    emqx:hook('session.discarded',   {?MODULE, on_session_discarded, [Env]}),
    emqx:hook('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}),
    emqx:hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    emqx:hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    emqx:hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    emqx:hook('message.acked',       {?MODULE, on_message_acked, [Env]}),
    emqx:hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}).



connect_db(Options) ->
    case mysql:start_link(Options) of
        {ok, Pid} -> {ok, Pid};
        ignore -> {error, ignore};
        {error, Reason = {{_, {error, econnrefused}}, _}} ->
            io:format("[MySQL] Can't connect to MySQL server: Connection refused."),
            {error, Reason};
        {error, Reason = {ErrorCode, _, Error}} ->
            io:format("[MySQL] Can't connect to MySQL server: ~p - ~p", [ErrorCode, Error]),
            {error, Reason};
        {error, Reason} ->
            io:format("[MySQL] Can't connect to MySQL server: ~p", [Reason]),
            {error, Reason}
    end.

gen_kafka_client_id(BootstrapBrokers) ->
    {ok, binary_to_atom(BootstrapBrokers)}.

check_then_build_kafka_connection(BootstrapBrokers) ->
    {ok, ProducerClientId} = gen_kafka_client_id(BootstrapBrokers),
    % io:format("[KAFKA] BootstrapBrokers: ~p ->ClientId:~p~n", [BootstrapBrokers, ProducerClientId]),
    case erlkaf:get_stats(ProducerClientId) of
        {error, Reason} ->
            ProducerConfig = [
                {bootstrap_servers, BootstrapBrokers},
                {delivery_report_only_error, true},
                {delivery_report_callback, ?MODULE}
            ],
            ok = erlkaf:create_producer(ProducerClientId, ProducerConfig),
            ets:insert(?ETS_TABLE_CACHE, {BootstrapBrokers, ProducerClientId}),
            io:format("[KAFKA] BootstrapBrokers: ~p ProducerClientId create success !!~n", [BootstrapBrokers]);
        {ok, []} ->
            io:format("[KAFKA] BootstrapBrokers ~p ProducerClientId existed~n", [BootstrapBrokers])
    end.


mapping_cache_interval_refresh() ->
    PId = self(),
    io:format("[MYSQL] mapping_cache_interval_refresh start... ~n"),
    {ok, Options} = application:get_env(?APP, mysql_options),
    io:format("[MYSQL] connection params: ~p ~n", [Options]),
    {ok, MysqlClient} = connect_db(Options),
    {ok, ColumnNames, Rows} = mysql:query(MysqlClient, <<"SELECT 1 as TEST">>),
    io:format("[MYSQL] exec result column_name:~p  rows:~p  ~n", [ColumnNames, Rows]),

    % mqtt_kafka_mapping_new config part 1
    {ok, _, TopicRows} = mysql:query(MysqlClient, <<"SELECT mqtt_topic_prefix, kafka_topic, LENGTH(mqtt_topic_prefix), content_type, kafka_broker_list from mqtt_kafka_mapping_new where is_active=1 and is_delete=0 and mqtt_topic_prefix not in ('__DEFAULT_TOPIC__', '__CONNECTION_STATE_TOPIC__') order by match_priority asc">>),
    lists:foreach(
        fun(X) ->
           [_, _, _, _, KafkaBrokerList] = X,
           check_then_build_kafka_connection(KafkaBrokerList)
    end, TopicRows),
    ets:insert(?ETS_TABLE_CACHE, {?MQTT_KAFKA_MAPPING_LIST, TopicRows}),
    [{_, MqttKafkaTopicMappingRows}] = ets:lookup(?ETS_TABLE_CACHE, ?MQTT_KAFKA_MAPPING_LIST),

    % mqtt_kafka_mapping_new config part 2
    {ok, _, DefaultTopicRows} = mysql:query(MysqlClient, <<"SELECT mqtt_topic_prefix, kafka_topic, content_type, kafka_broker_list from mqtt_kafka_mapping_new where mqtt_topic_prefix = '__DEFAULT_TOPIC__'">>),
    lists:foreach(
        fun(X) ->
           [_, _, _, KafkaBrokerList] = X,
           io:format("[MYSQL] query row=> ~p ~n", [X]),
           check_then_build_kafka_connection(KafkaBrokerList),
           ets:insert(?ETS_TABLE_CACHE, {?DEFAULT_TOPIC_MAPPING, X})
    end, DefaultTopicRows),
    % [{_, DEFAULT_TOPIC_MAPPING_ROWS}] = ets:lookup(?ETS_TABLE_CACHE, ?DEFAULT_TOPIC_MAPPING),
    % io:format("[MYSQL] DEFAULT_TOPIC_MAPPING_ROWS:~p ~n", [DEFAULT_TOPIC_MAPPING_ROWS]),

    % mqtt_kafka_mapping_new config part 3
    {ok, _, ConnectionTopicRows} = mysql:query(MysqlClient, <<"SELECT mqtt_topic_prefix, kafka_topic, content_type, kafka_broker_list from mqtt_kafka_mapping_new where mqtt_topic_prefix = '__CONNECTION_STATE_TOPIC__'">>),
    lists:foreach(
        fun(X) ->
           [_, _, _, KafkaBrokerList] = X,
           io:format("[MYSQL] query row=> ~p ~n", [X]),
           check_then_build_kafka_connection(KafkaBrokerList),
           ets:insert(?ETS_TABLE_CACHE, {?CONNECTION_TOPIC_MAPPING, X})
    end, ConnectionTopicRows),
    % [{_, CONNECTION_STATUS_TOPIC_ROWS}] = ets:lookup(?ETS_TABLE_CACHE, ?CONNECTION_TOPIC_MAPPING),
    % io:format("[MYSQL] CONNECTION_TOPIC_MAPPING:~p ~n", [CONNECTION_STATUS_TOPIC_ROWS]),

    mysql:stop(MysqlClient),
    % {ok, TopicMappingRefresh} = application:get_env(?APP, topic_mapping_refesh),
    % io:format("[MYSQL] mapping_cache_interval_refresh done ,refresh after ~B ms~n", [TopicMappingRefresh]),
    % {ok, TRef} = timer:apply_after(TopicMappingRefresh, ?MODULE, mapping_cache_interval_refresh, []),
    % io:format("启动定时任务: ~p", [TRef]),
    % put(?MAPPING_CACHE_REFRESH_TIMER, TRef),
    ok.

delivery_report(DeliveryStatus, Message) ->
    io:format("received delivery report: ~p ~n", [{DeliveryStatus, Message}]),
    ok.

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo = #{clientid := ClientId}, Props, _Env) ->
    io:format("Client(~s) connect, ConnInfo: ~p, Props: ~p~n",
              [ClientId, ConnInfo, Props]),
    {ok, Props}.

on_client_connack(ConnInfo = #{clientid := ClientId}, Rc, Props, _Env) ->
    io:format("Client(~s) connack, ConnInfo: ~p, Rc: ~p, Props: ~p~n",
              [ClientId, ConnInfo, Rc, Props]),
    {ok, Props}.

on_client_connected(ClientInfo=#{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost}, ConnInfo, _Env) ->
    F = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,
    Json = jiffy:encode({[
        {type, <<"connected">>},
        {client_id, F(ClientId)},
        {username, F(Username)},
        {peerhost, ntoa(PeerHost)},
        {cluster_node, a2b(node())},
        {timestamp, erlang:system_time(millisecond)}
    ]}),
    io:format("<<kafka json>>Client(~s) connected, Json: ~s~n", [ClientId, Json]),
    ok = produce_status(ClientId, Json),
    % io:format("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
    %          [ClientId, ClientInfo, ConnInfo]).
    ok.

on_client_disconnected(ClientInfo = #{
        clientid := ClientId,
        username := Username}, ReasonCode, ConnInfo, _Env) ->
    % io:format("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n", [ClientId, ReasonCode, ClientInfo, ConnInfo]),

    F2 = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,

    Json = jiffy:encode({[
        {type, <<"disconnected">>},
        {client_id, F2(ClientId)},
        {username, F2(Username)},
        {cluster_node, a2b(node())},
        {reason, a2b(ReasonCode)},
        {timestamp, erlang:system_time(millisecond)}
    ]}),
    io:format("<<kafka json>>Client(~s) disconnected, Json: ~s~n", [ClientId, Json]),
    ok = produce_status(ClientId, Json),
    ok.

on_client_authenticate(_ClientInfo = #{clientid := ClientId}, Result, _Env) ->
    io:format("Client(~s) authenticate, Result:~n~p~n", [ClientId, Result]),
    {ok, Result}.

on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
    io:format("Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
              [ClientId, PubSub, Topic, Result]),
    {ok, Result}.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

on_session_resumed(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) resumed, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_discarded(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is discarded. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
    io:format("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
              [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{
           id = <<I1:64, I2:48, I3:16>> = _MsgId,
           headers = #{peerhost := PeerHost, username := Username},
           from = ClientId,
           qos = QoS,
           flags = #{dup := Dup, retain := Retain},
           topic = MqttTopic,
           payload = Payload,
           timestamp = Ts}, _Env) ->
    [{_, MqttKafkaTopicMappingRows}] = ets:lookup(?ETS_TABLE_CACHE, ?MQTT_KAFKA_MAPPING_LIST),
    Id =  I1 + I2 + I3,
    IsMatch = lists:any(
        fun(MappingRow) ->
            [MqttTopicPrefix, KafkaTopic, PrefixLength, ContentType, BootstrapBrokers] = MappingRow,
            case binary:longest_common_prefix([MqttTopic, MqttTopicPrefix]) == PrefixLength of 
                true ->
                    gen_json_then_publish(MqttTopic, KafkaTopic, ContentType, BootstrapBrokers, ClientId, Id, PeerHost, Username, Payload, QoS, Dup, Retain, Ts),
                    true;
                false -> false
            end
        end, MqttKafkaTopicMappingRows),

    case IsMatch of
        false -> 
            [{_, [MqttTopicPrefix, KafkaTopic, ContentType, BootstrapBrokers]}] = ets:lookup(?ETS_TABLE_CACHE, ?DEFAULT_TOPIC_MAPPING),
            gen_json_then_publish(MqttTopic, KafkaTopic, ContentType, BootstrapBrokers, ClientId, Id, PeerHost, Username, Payload, QoS, Dup, Retain, Ts);
        true -> ok
    end,      
    {ok, Message}.

gen_json_then_publish(MqttTopic, KafkaTopic, ContentType, BootstrapBrokers, ClientId, Id,PeerHost, Username, Payload, QoS, Dup, Retain, Ts) ->
    % case is_binary(Payload) of 
    %     true -> io:format("Payload:~p is binary ~n", [Payload]);
    %     false -> io:format("Payload:~p not binary ~n", [Payload])
    % end,
    F1 = fun(X) -> case X of  true ->1; _ -> 0 end end,
    F2 = fun (X) -> case X of undefined -> <<"undefined">>; _ -> X  end end,
    case ContentType == <<"raw">> of
        true ->
            Json = jiffy:encode({[
                {type, <<"published">>},
                {version, 1}, % 扩展协议使用
                {id, Id},
                {client_id, ClientId},
                % {peerhost, ntoa(PeerHost)},
                {username, F2(Username)},
                {topic, MqttTopic},
                {payload, Payload},%  raw_value
                {payload_type, ContentType}, % raw
                {qos, QoS},
                {dup, F1(Dup)},
                {retain, F1(Retain)},
                {cluster_node, a2b(node())},
                {timestamp, Ts}
            ]}),
            case is_binary(Json) of   % for handle big value json
                true ->
                    ok = produce_points(ClientId, MqttTopic, KafkaTopic, BootstrapBrokers, Json);
                false ->
                    NewJson = iolist_to_binary(Json),
                    ok = produce_points(ClientId, MqttTopic, KafkaTopic, BootstrapBrokers, NewJson)
            end;
        false ->
            Json = jiffy:encode({[
                {type, <<"published">>},
                {version, 1}, % 扩展协议使用
                {id, Id},
                {client_id, ClientId},
                % {peerhost, ntoa(PeerHost)},
                {username, F2(Username)},
                {topic, MqttTopic},
                {payload, binary:encode_hex(Payload)}, % hex_value   string
                {payload_type, ContentType}, % hex
                {qos, QoS},
                {dup, F1(Dup)},
                {retain, F1(Retain)},
                {cluster_node, a2b(node())},
                {timestamp, Ts}
            ]}),
            case is_binary(Json) of % for handle big value json
                true ->
                    ok = produce_points(ClientId, MqttTopic, KafkaTopic, BootstrapBrokers, Json);
                false ->
                    NewJson = iolist_to_binary(Json),
                    ok = produce_points(ClientId, MqttTopic, KafkaTopic, BootstrapBrokers, NewJson)
            end
        end,
    ok.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
    % io:format("Message dropped by node ~s due to ~s: ~s~n",[Node, Reason, emqx_message:format(Message)])
    ok.

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    % io:format("Message delivered to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message acked by client(~s): ~s~n", [ClientId, emqx_message:format(Message)]).

produce_status(ClientId, Json) ->
    [{_, [MqttTopicPrefix, KafkaTopic, ContentType, BootstrapBrokers]}] =  ets:lookup(?ETS_TABLE_CACHE, ?CONNECTION_TOPIC_MAPPING),
    {ok, ProducerClientId} = gen_kafka_client_id(BootstrapBrokers),
    ok = erlkaf:produce(ProducerClientId, KafkaTopic, ClientId, Json).

produce_points(ClientId, MqttTopic, KafkaTopic, BootstrapBrokers, Json) ->
    {ok, ProducerClientId} = gen_kafka_client_id(BootstrapBrokers),
    ok = erlkaf:produce(ProducerClientId, KafkaTopic, MqttTopic, Json).

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

a2b(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8);
a2b(A) -> A.

%% 从配置中获取当前Kafka的初始broker配置
get_bootstrap_brokers() ->
    application:get_env(?APP, bootstrap_brokers_list).


%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connect',      {?MODULE, on_client_connect}),
    emqx:unhook('client.connack',      {?MODULE, on_client_connack}),
    emqx:unhook('client.connected',    {?MODULE, on_client_connected}),
    emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    emqx:unhook('client.check_acl',    {?MODULE, on_client_check_acl}),
    emqx:unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
    emqx:unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    emqx:unhook('session.created',     {?MODULE, on_session_created}),
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('session.resumed',     {?MODULE, on_session_resumed}),
    emqx:unhook('session.discarded',   {?MODULE, on_session_discarded}),
    emqx:unhook('session.takeovered',  {?MODULE, on_session_takeovered}),
    emqx:unhook('session.terminated',  {?MODULE, on_session_terminated}),
    emqx:unhook('message.publish',     {?MODULE, on_message_publish}),
    emqx:unhook('message.delivered',   {?MODULE, on_message_delivered}),
    emqx:unhook('message.acked',       {?MODULE, on_message_acked}),
    emqx:unhook('message.dropped',     {?MODULE, on_message_dropped}),

    % It is ok to leave application there.
    % close all client kafka client list
    [{_, MqttKafkaTopicMappingRows}] = ets:lookup(?ETS_TABLE_CACHE, ?MQTT_KAFKA_MAPPING_LIST),
    lists:foreach(
        fun(X) ->
            [_, _, _, _, BootstrapBrokers] = X,
            {ok, ProducerClientId} = gen_kafka_client_id(BootstrapBrokers),
            case erlkaf:get_stats(ProducerClientId) of
                {error, Reason} ->
                    io:format("not exist client: ~p  ~n", [ProducerClientId]);
                {ok, []} -> erlkaf:stop_client(ProducerClientId)
            end
        end, MqttKafkaTopicMappingRows),
    ets:delete(?ETS_TABLE_CACHE),
    % timer:cancle(get(?MAPPING_CACHE_REFRESH_TIMER)),
    % erase(?MAPPING_CACHE_REFRESH_TIMER),
    %ok = erlkaf:stop(client_producer),
    io:format("Finished all unload works.").
