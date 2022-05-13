package app;

/**
 * @author liangjianxiang
 * @date 2022/5/13 17:10
 */

import bean.ProductBrand;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import util.FlinkUtils;

import java.util.Properties;


public class Flink2MK {
    public static void main(String[] args) throws Exception {
        //获取flink执行环境
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        //读取kafka数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.49.0.143:9092")
                .setTopics("product_brand")
                .setGroupId("com/lingxing/test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.49.0.143:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "com/lingxing/test");
        properties.setProperty("flink.partition-discovery.interval-millis", "10000");

        FlinkKafkaConsumerBase<String> productBrand = new FlinkKafkaConsumer<String>("product_brand", new SimpleStringSchema(), properties)
                .setStartFromEarliest();

        //转换kafka数据为java Bean对象
//        SingleOutputStreamOperator<ProductBrand> test = env
//                .addSource(productBrand)
//                .map(jsonStr -> {
//                    Struct records = (Struct) JSONObject.parse(jsonStr);
//                    String after = (String) records.get("after");
//
//                    return JSONUtil.toBean(after, ProductBrand.class);
//                });

        SingleOutputStreamOperator<ProductBrand> test = env
                .addSource(new FlinkKafkaConsumer<String>("product_brand", new SimpleStringSchema(), properties).setStartFromEarliest())
                .map(jsonStr -> {
                    JSONObject values = JSONObject.parseObject(jsonStr);
                    String op = values.get("op").toString();
                    ProductBrand record = null;
                    switch (op) {
                        case "c":
                        case "r":
                        case "u":
                            String after = values.get("after").toString();
                            record = JSONUtil.toBean(after, ProductBrand.class);
                            break;
                        case "d":
                            String before = values.get("before").toString();
                            record = JSONUtil.toBean(before, ProductBrand.class);
                            break;
                    }
                    return record;
                });

        //处理后数据jdbc方式写入mysql
        DataStreamSink stringDataStreamSink = test.addSink(
                JdbcSink.sink(
                        "insert into product_brand (id,zid,sort,title,gmt_create,gmt_modified) values (?,?,?,?,?,?)",                       // mandatory
                        (ps, records) -> {
                            ps.setInt(1, checkNull(records.getId()));
                            ps.setInt(2, checkNull(records.getZid()));
                            ps.setInt(3, checkNull(records.getSort()));
                            ps.setString(4, checkNull(records.getTitle()));
                            ps.setString(5, checkNull(records.getGmtCreate()));
                            ps.setString(6, checkNull(records.getGmtModified()));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(10)
                                .withMaxRetries(10)
                                .build(),                  // optional
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(String.format("jdbc:mysql://%s:%s/%s", "10.49.0.143", "3306", "test"))
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("root")
                                .build()
                )
        );
        test.print();
        env.execute();

    }

    public static <T> T checkNull(T record) {
        if (null == record) {
            return (T) "";
        } else {
            return record;
        }
    }
}

