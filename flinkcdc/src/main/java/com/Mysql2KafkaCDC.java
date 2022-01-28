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
import com.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

public class Mysql2KafkaCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.setRestartStrategy(RestartStrategies.noRestart());

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

        DebeziumSourceFunction<ObjectNode> mysqlSource = MySqlSource.<ObjectNode>builder()
                .hostname("rm-bp146k5r8kw09t21xho.mysql.rds.aliyuncs.com")
                .port(3306)
                .username("star_test")
                .password("STARcai01230")
                .databaseList("gmall")
                .tableList("gmall.user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema() {

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(ObjectNode.class);
                    }

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);


                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        String ts_ms = value.get("ts_ms").toString();
                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        ObjectNode data = objectMapper.createObjectNode();

                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o == null ? null : o.toString());
                        }
                        //创建JSON对象用于存储数据信息
                        ObjectNode objectNode = objectMapper.createObjectNode();

                        //创建JSON对象用于封装最终返回值数据信息
                        objectNode.put("operation", operation.toString().toLowerCase());
                        objectNode.put("data", data);
                        objectNode.put("database", db);
                        objectNode.put("table", tableName);
                        objectNode.put("timestamp", ts_ms);

                        String result = objectMapper.writeValueAsString(objectNode);

                        System.out.println("result:" + result);
                        //发送数据至下游
                        collector.collect(objectNode);
                    }
                })
                .build();

        DataStreamSource<ObjectNode> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.addSink(MyKafkaUtil.getKafkaSinkbySchema(new KafkaSerializationSchema<ObjectNode>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("start kafka sink");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(ObjectNode element, @Nullable Long timestamp) {
                String sink_table = element.get("table").asText();
                JsonNode data = element.get("data");
                System.out.println("kafka talbe:"+sink_table);
                return new ProducerRecord<>(sink_table,data.toString().getBytes());
            }
        }));

        env.execute("flink-cdc-kafka");
    }
}
