package app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import util.FlinkUtils;


public class Flink2Clickhouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        MySqlSource<String> mySqlSource = MySqlSource.builder()
                .hostname("8.134.35.221")
                .port(3306)
                .databaseList("test")
                .tableList("test.product_brand")
                .username("root")
                .password("root")
                .deserializer((DebeziumDeserializationSchema)new UserBeanDebeziumDeserializationSchema())
                .build();

        DataStreamSource mysqlStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlSource");

        mysqlStreamSource.map(new MapFunction<String,String>() {
            @Override
            public String map(String o) throws Exception {
                System.out.println(o);
                return o;
            }
        });

        env.execute();
        System.out.println(mysqlStreamSource);


    }

    static class UserBeanDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String>{

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            Struct value = (Struct) sourceRecord.value();
            String operator = value.getString("op");

            collector.collect(sourceRecord.toString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
