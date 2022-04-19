package app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class Flink2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ckp容灾机制
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                100, // 尝试重启的次数
                Time.of(60, TimeUnit.SECONDS) // 延时
        ));
        //ckp频率
        env.enableCheckpointing(30 * 1000L);
        //ckp精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //ckp间隔时长
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000L);
        //ckp超时限制
        env.getCheckpointConfig().setCheckpointTimeout(15 * 60 * 1000L);
        //ckp容错次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        //ckp并存数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        SimpleStringSchema simpleStringSchema = new SimpleStringSchema();
        KafkaSource build = KafkaSource.builder()
                .setBootstrapServers("10.49.0.143:9092")
                .setTopics("ods_order_mws1")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer((DeserializationSchema) simpleStringSchema)
                .build();

        DataStreamSource test = env.fromSource(build, WatermarkStrategy.noWatermarks(), "test");

        test.print();

        env.execute();

//        String orderMws =
//                "                id                              INT,\n" +
//                "                zid                             INT,\n" +
//                "                sid                             INT,\n" +
//                "                wid                             INT,\n" +
//                "                latest_ship_date                STRING,\n" +
//                "                order_type                      STRING,\n" +
//                "                purchase_date                   STRING,\n" +
//                "                purchase_date_locale            DATE,\n" +
//                "                purchase_date_local             TIMESTAMP(3),\n" +
//                "                amazon_order_id                 STRING,\n" +
//                "                buyer_email                     STRING,\n" +
//                "                is_replacement_order            STRING,\n" +
//                "                replaced_order_id               STRING,\n" +
//                "                last_update_date                STRING,\n" +
//                "                ship_service_level              STRING,\n" +
//                "                number_of_items_shipped         INT,\n" +
//                "                order_status                    STRING,\n" +
//                "                sales_channel                   STRING,\n" +
//                "                order_channel                   STRING,\n" +
//                "                is_business_order               INT,\n" +
//                "                number_of_items_unshipped       INT,\n" +
//                "                payment_execution_detail        STRING,\n" +
//                "                payment_method_details          STRING,\n" +
//                "                payment_method_detail           STRING,\n" +
//                "                buyer_name                      STRING,\n" +
//                "                buyer_county                    STRING,\n" +
//                "                buyer_tax_info                  STRING,\n" +
//                "                company_legal_name              STRING,\n" +
//                "                taxing_region                   STRING,\n" +
//                "                tax_classifications             STRING,\n" +
//                "                order_total                     STRING,\n" +
//                "                order_total_currency_code       STRING,\n" +
//                "                order_total_amount              decimal(10, 2),\n" +
//                "                is_premium_order                INT,\n" +
//                "                earliest_ship_date              STRING,\n" +
//                "                marketplace_id                  STRING,\n" +
//                "                fulfillment_channel             STRING,\n" +
//                "                payment_currency_code           STRING,\n" +
//                "                payment_amount                  decimal,\n" +
//                "                payment_method                  STRING,\n" +
//                "                shipping_address                STRING,\n" +
//                "                state_or_region                 STRING,\n" +
//                "                city                            STRING,\n" +
//                "                county                          STRING,\n" +
//                "                district                        STRING,\n" +
//                "                country_code                    STRING,\n" +
//                "                postal_code                     STRING,\n" +
//                "                name                            STRING,\n" +
//                "                address_line1                   STRING,\n" +
//                "                address_line2                   STRING,\n" +
//                "                address_line3                   STRING,\n" +
//                "                phone                           STRING,\n" +
//                "                is_prime                        INT,\n" +
//                "                shipment_service_level_category STRING,\n" +
//                "                shipped_by_amazon_tfm           STRING,\n" +
//                "                tfm_shipment_status             STRING,\n" +
//                "                cba_displayable_shipping_label  STRING,\n" +
//                "                earliest_delivery_date          STRING,\n" +
//                "                latest_delivery_date            STRING,\n" +
//                "                purchase_order_number           STRING,\n" +
//                "                seller_order_id                 STRING,\n" +
//                "                sync_time                       INT,\n" +
//                "                next_sync_time                  INT,\n" +
//                "                remark                          STRING,\n" +
//                "                create_time                     INT,\n" +
//                "                update_time                     INT,\n" +
//                "                gmt_modified                    TIMESTAMP(3),\n" +
//                "                gmt_create                      TIMESTAMP(3),\n" +
//                "                is_return                       INT,\n" +
//                "                request_id                      STRING,\n" +
//                "                data_from                       STRING,\n" +
//                "                is_mcf_order                    INT,\n" +
//                "                is_self                         INT,\n" +
//                "                earliest_ship_date_locale       DATE,\n" +
//                "                `ts` TIMESTAMP(3),\n" +
//                "                primary key (zid,sid,`amazon_order_id`) NOT ENFORCED)\n";
//
//        tEnv.executeSql(
//                " CREATE TABLE `order_mws_kafka` (\n" +
//                orderMws +
//                "with(" +
//                        "        'connector' = 'kafka',\n" +
//                        "        'topic' = 'ods_order_mws1',\n" +
//                        "        'properties.bootstrap.servers' = '10.49.0.143:9092',\n" +
//                        "        'properties.group.id' = 'test',\n" +
//                        "        'scan.startup.mode' = 'earliest-offset',\n" +
//                        "        'debezium-json.ignore-parse-errors' = 'true',\n" +
//                        "        'format' = 'debezium-json',\n" +
//                        "        'scan.topic-partition-discovery.interval' = '600000'\n" +
//                        ")");
//
//        tEnv.executeSql(
//                " CREATE TABLE `order_mws_mysql` (\n" +
//                orderMws +
//                        "WITH (\n" +
//                        " 'connector' = 'mysql-cdc',\n" +
//                        " 'hostname' = '10.49.0.86',\n" +
//                        " 'port' = '3306',\n" +
//                        " 'username' = 'bdp',\n" +
//                        " 'password' = 'akd_bdp',\n" +
//                        " 'database-name' = 'dev_erp',\n" +
//                        " 'table-name' = 'order_mws_bak',\n" +
//                        " 'debezium.snapshot.locking.mode' = 'none'\n" +
//                        ")"
//        );
//
//        tEnv.executeSql("select * from order_mws_kafka\n" +
//                "where zid is not null and sid is not null and `amazon_order_id` is not null and `amazon_order_id` <> ''");

    }
}
