package test;

import bean.ProductBrand;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import util.FlinkUtils;

public class a {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StatementSet statementSet = FlinkUtils.getStatementSet();
        StreamTableEnvironment tEnv = FlinkUtils.gettEnv();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("")
                .port(3306)
                .databaseList("dev_erp")
                .tableList("dev_erp.product_brand_bak")
                .username("")
                .password("")
                .deserializer(new UserDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");

        SingleOutputStreamOperator<ProductBrand> outputStreamOperator = mysqlSource.map(new MapFunction<String, ProductBrand>() {
            @Override
            public ProductBrand map(String s) throws Exception {
                JSONObject parseObject = JSONObject.parseObject(s);
                String value = parseObject.get("value").toString();
                ProductBrand productBrand = JSON.parseObject(value, ProductBrand.class);
                return productBrand;
            }
        });
        outputStreamOperator.addSink(new PrintSinkFunction<ProductBrand>());

        env.execute("test a");
    }

    public static class UserDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        public UserDebeziumDeserializationSchema() {
        }

        public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
            Struct value = (Struct) record.value();
            Struct currnet;
            JSONObject date = new JSONObject();
            JSONObject records = new JSONObject();
            String op = value.getString("op");
            Struct after = (Struct) value.get("after");
            Struct before = (Struct) value.get("before");
            if ("c".equals(op) || "r".equals(op) || "u".equals(op)) {
                currnet = value.getStruct("after");
            } else {
                currnet = value.getStruct("before");
            }
            date.put("op", op);
            for (Field field : currnet.schema().fields()) {
                records.put(field.name(), currnet.get(field));
            }
            date.put("value",records);
            out.collect(date.toJSONString());
        }

        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
