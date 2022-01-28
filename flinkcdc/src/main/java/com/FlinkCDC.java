package com;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.ParseException;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImplConstants.TBLPROPERTIES;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //flink-cdc将读取binlog的位置信息以状态的方式保存在ck，如果想要做到断点续传，需要从ck或者savepoint启动程序
        //开启ck，间隔5s,并指定语义
        env.enableCheckpointing(5000l,CheckpointingMode.EXACTLY_ONCE);

        env.getConfig().setAutoWatermarkInterval(2000);

        //设置任务关闭时保留最后一次ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //指定从ck自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000l));

        //设置状态后段
        env.setStateBackend(new FsStateBackend("hdfs://node:8020/flink-cdc"));

        System.setProperty("HADOOP_USER_NAME","star");


        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParanamerModule());

        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
//        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES,false);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node")
                .port(3306)
                .databaseList("ecommerce")
                .tableList("ecommerce.t_user")
                .username("root")
                .password("STARcai01230")
//                .deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(new DebeziumDeserializationSchema(){

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(String.class);
                    }

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
//获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        ObjectNode data = objectMapper.createObjectNode();

                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o.toString());
                        }
                        //创建JSON对象用于存储数据信息
                        ObjectNode objectNode = objectMapper.createObjectNode();

                        //创建JSON对象用于封装最终返回值数据信息
                        objectNode.put("operation", operation.toString().toLowerCase());
                        objectNode.put("data", data);
                        objectNode.put("database", db);
                        objectNode.put("table", tableName);

                        String result = objectMapper.writeValueAsString(objectNode);
                        //发送数据至下游
                        collector.collect(result);
                    }
                })
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(mySqlSource);

        mysqlDS.print(">>>>>>>>>>>>>>>>>>>>>>>>");


        String[] fieldNames  =
                {"id","user_name","user_password","user_phone","create_time","update_time"};

        TypeInformation[] types =
                {Types.INT,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING};

        SingleOutputStreamOperator<Row> ds2 = mysqlDS.map(new
           MapFunction<String, Row>() {
               @Override
               public Row map(String value) throws Exception {
                   System.out.println(">>>>>>>>>>>>>>>>:"+value);
                   JsonNode jsonNode = objectMapper.readValue(value, JsonNode.class);
                   int arity = fieldNames.length;
                   JsonNode data = jsonNode.get("data");
                   Row row = new Row(arity);
                   row.setField(0, data.get("id").asText());
                   row.setField(1, data.get("id").asText());
                   row.setField(2, data.get("id").asText());
                   row.setField(3, data.get("id").asText());
                   row.setField(4, data.get("id").asText());
                   row.setField(5, data.get("id").asText());
                   row.setField(6, data.get("id").asText());
                   row.setField(7, jsonNode.get("operation").asText());
                   return row;
               }
           }, new RowTypeInfo(types, fieldNames));
        // 设置水印
        SingleOutputStreamOperator<Row> ds3 =
                ds2.assignTimestampsAndWatermarks(

                    WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new
                       SerializableTimestampAssigner<Row>() {
                           @Override
                           public long extractTimestamp(Row element, long
                                   recordTimestamp) {
                               String create_time = (String)
                                       element.getField(2);
                               FastDateFormat dateFormat =
                                       FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                               try {
                                   long time =
                                           dateFormat.parse(create_time).getTime();
                                   return time;
                               } catch (ParseException e) {
                                   e.printStackTrace();
                               }
                               return 0;
                           }
                       })
                );

        //设置flinksql环境
        /*EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //创建table env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        //设置checkpoint模型
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint间隔
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(1));

        String catalogName = "devHive";
        //创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, "ecommerce", "/Users/star/develop/module/apache-hive-3.1.2-bin/conf");

        //注册hive catalog
        tableEnv.registerCatalog(catalogName,hiveCatalog);

        //使用hive catalog
        tableEnv.useCatalog(catalogName);

        //创建mysql cdc数据源
        tableEnv.executeSql("create database if not exists cdc");
        tableEnv.executeSql("create table cdc.order_info(id bigint, user_id bigint," +
                "create_time timestamp,opercate_time,province_id int,order_status string," +
                "total_amount decimal(10,5)) with ('connector' = 'mysql-cdc' , " +
                "'hostname'='node','port'='3306','username'='root'," +
                "'password'='STARcai01230','database-name'='ecommerce'," +
                "'table-name='*')");

        tableEnv.executeSql("create database if not exists kafka");
        tableEnv.executeSql("drop table if exists kafka.order_info");
        tableEnv.executeSql("CREATE TABLE kafka.order_info (\n" +
                "id BIGINT,\n" +
                "user_id BIGINT,\n" +
                "create_time TIMESTAMP,\n" +
                "operate_time TIMESTAMP,\n" +
                "province_id INT,\n" +
                "order_status STRING,\n" +
                "total_amount DECIMAL(10, 5)\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'order_info',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'properties.bootstrap.servers' = 'node:9092'," +
        "'format' = 'changelog-json'\n" +
                ")");

        tableEnv.executeSql("insert into kafka.order_info " +
                "select id ,user_id,create_time,operate_time,province_id," +
                "order_status,total_amount from cdc.order_info");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node:9092");
        FlinkKafkaConsumerBase<String> consumer = new FlinkKafkaConsumer<String>("order_info", new SimpleStringSchema(), props)
                .setStartFromEarliest();

        DataStreamSource<String> streamSource = env.addSource(consumer);

        String[] fieldNames  =
                {"id","user_id","create_time","operate_time","province_id","order_status","total_amount","op"};

        TypeInformation[] types =
                {Types.LONG,Types.LONG,Types.STRING,Types.STRING,Types.INT,Types.INT,Types.DOUBLE,Types.STRING};


        SingleOutputStreamOperator<Row> ds2 = streamSource.map(new
           MapFunction<String, Row>() {
               @Override
               public Row map(String value) throws Exception {
                   JsonNode jsonNode = objectMapper.readValue(value, JsonNode.class);
                   System.out.println(jsonNode);
                   int arity = fieldNames.length;
                   Row row = new Row(arity);
                   row.setField(0, changelogVO.getData().getId());
                   row.setField(1, changelogVO.getData().getUserId());
                   row.setField(2, changelogVO.getData().getCreateTime());
                   row.setField(3, changelogVO.getData().getOperateTime());
                   row.setField(4, changelogVO.getData().getProviceId());
                   row.setField(5, changelogVO.getData().getOrderStatus());
                   row.setField(6, changelogVO.getData().getTotalAmount());
                   String operation = getOperation(op);
                   row.setField(7, operation);
                   return row;
               }

               private String getOperation(String op) {
                   String operation = "INSERT";
                   for (RowKind rk : RowKind.values()) {
                       if (rk.shortString().equals(op)) {
                           switch (rk) {
                               case UPDATE_BEFORE:
                                   operation = "UPDATE-BEFORE";
                                   break;
                               case UPDATE_AFTER:
                                   operation = "UPDATE-AFTER";
                                   break;
                               case DELETE:
                                   operation = "DELETE";
                                   break;
                               case INSERT:
                               default:
                                   operation = "INSERT";
                                   break;
                           }
                           break;
                       }
                   }
                   return operation;
               }
           }, new RowTypeInfo(types, fieldNames));
        // 设置水印
        SingleOutputStreamOperator<Row> ds3 =
        ds2.assignTimestampsAndWatermarks(

        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new
       SerializableTimestampAssigner<Row>() {
           @Override
           public long extractTimestamp(Row element, long
                   recordTimestamp) {
               String create_time = (String)
                       element.getField(2);
               FastDateFormat dateFormat =
                       FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
               try {
                   long time =
                           dateFormat.parse(create_time).getTime();
                   return time;
               } catch (ParseException e) {
                   e.printStackTrace();
               }
               return 0;
           }
       })
                );
        tableEnv.createTemporaryView("merged_order_info", ds3);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.order_info");
        tableEnv.executeSql("CREATE TABLE ods.order_info (\n" +
                        "  id BIGINT,\n" +
                        "   user_id BIGINT,\n" +
                        "   create_time STRING,\n" +
                        "   operate_time STRING,\n" +
                        "   province_id INT,\n" +
                        "   order_status INT,\n" +
                        "   total_amount DOUBLE,\n" +
                        "   op STRING \n" +
                        ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
        "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.delay'='1 min',\n" +
                "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
        ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO ods.order_info\n" +
                        "SELECT \n" +
                        "id,\n" +
                        "user_id,\n" +
                        "create_time,\n" +
                        "operate_time,\n" +
                        "province_id,\n" +
                        "order_status,\n" +
                        "total_amount,\n" +
                        "op,\n" +
                        "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as dt,\n" +
        "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-ddHH:mm:ss'),'HH') as hr\n" +
        "FROM merged_order_info"
        );*/

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
