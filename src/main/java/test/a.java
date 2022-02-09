package test;

import bean.ProductBrand;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import util.FlinkUtils;

public class a {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StatementSet statementSet = FlinkUtils.getStatementSet();
        StreamTableEnvironment tEnv = FlinkUtils.gettEnv();

        MySqlSource<String> mySqlSource = MySqlSource.builder()
                .hostname("8.134.35.221")
                .port(3306)
                .databaseList("test")
                .tableList("test.product_brand")
                .username("root")
                .password("root")
//                .deserializer((DebeziumDeserializationSchema)new JsonDebeziumDeserializationSchema())
                .deserializer((DebeziumDeserializationSchema)new UserDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");

//        SingleOutputStreamOperator productbrandStream = mysqlSource.map(new MapFunction<String, ProductBrand>() {
//            @Override
//            public ProductBrand map(String s) throws Exception {
//                JSONObject jsonObject = JSON.parseObject(s);
//                ProductBrand productbrand = JSON.parseObject(jsonObject.get("after").toString(), ProductBrand.class);
//                return productbrand;
//            }
//        });


        mysqlSource.print();

//        productbrandStream.addSink()


        /*tEnv.executeSql("create table `product_brand` (\n" +
                "   id BIGINT,\n" +
                "   zid BIGINT,\n" +
                "   title STRING,\n" +
                "   sort BIGINT,\n" +
                "   gmt_modified TIMESTAMP,\n" +
                "   gmt_create TIMESTAMP," +
                "   primary key (`id`) NOT ENFORCED\n" +
                ")with(\n" +
                "            'connector' = 'mysql-cdc',\n" +
                "            'hostname' = '8.134.35.221',\n" +
                "            'port' = '3306',\n" +
                "            'username' = 'root',\n" +
                "            'password' = 'root',\n" +
                "            'database-name' = 'test',\n" +
                "            'table-name' = 'product_brand',\n" +
                "            'debezium.snapshot.mode' = 'initial',\n" +
                "            'debezium.snapshot.locking.mode' = 'none')");

        Table table = tEnv.sqlQuery("select * from product_brand");
        tEnv.toRetractStream(table, Row.class).print();*/
        env.execute("test a");
    }

    static class UserDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
            out.collect(record.toString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
