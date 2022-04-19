package test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkUtils;

public class SRTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        StreamTableEnvironment streamTableEnvironment = FlinkUtils.gettEnv();

        streamTableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `default_catalog`.`default_database`.`rule_group_mysql`(\n" +
                "  `id`\tBIGINT,\n" +
                "  `company_id`\tBIGINT,\n" +
                "  `store_id`\tINT,\n" +
                "  `name`\tSTRING,\n" +
                "  `status`\tINT,\n" +
                "  `created_by`\tINT,\n" +
                "  `modified_by`\tINT,\n" +
                "  `create_time`\tINT,\n" +
                "  `update_time`\tINT,\n" +
                "  `rule`\tSTRING,\n" +
                "  `criteria`\tSTRING,\n" +
                "  `type`\tINT,\n" +
                "  `remark`\tSTRING,\n" +
                "  `is_delete`\tINT,\n" +
                "  `gmt_create`\tTIMESTAMP(3),\n" +
                "  `gmt_modified`\tTIMESTAMP(3),\n" +
                "  PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = '10.49.0.86',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'bdp',\n" +
                "   'password' = 'akd_bdp',\n" +
                "   'database-name' = 'dev_erp',\n" +
                "   'table-name' = 'rule_group',\n" +
                "   'scan.startup.mode' = 'initial',\n" +
                "   'debezium.snapshot.locking.mode' = 'none'" +
                ")");

        streamTableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `default_catalog`.`default_database`.`rule_group_sr`(\n" +
                "  `id`\tBIGINT,\n" +
                "  `company_id`\tBIGINT,\n" +
                "  `store_id`\tINT,\n" +
                "  `name`\tSTRING,\n" +
                "  `status`\tINT,\n" +
                "  `created_by`\tINT,\n" +
                "  `modified_by`\tINT,\n" +
                "  `create_time`\tINT,\n" +
                "  `update_time`\tINT,\n" +
                "  `rule`\tSTRING,\n" +
                "  `criteria`\tSTRING,\n" +
                "  `type`\tINT,\n" +
                "  `remark`\tSTRING,\n" +
                "  `is_delete`\tINT,\n" +
                "  `gmt_create`\tTIMESTAMP(3),\n" +
                "  `gmt_modified`\tTIMESTAMP(3),\n" +
                "  PRIMARY KEY(`id`, `gmt_create`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'starrocks',\n" +
                "   'jdbc-url' = 'jdbc:mysql://10.49.8.41:9030',\n" +
                "   'load-url' = '10.49.8.41:8030',\n" +
                "   'database-name' = 'dev_erp2',\n" +
                "   'table-name' = 'rule_group',\n" +
                "   'username' = 'bigdata_user',\n" +
                "   'password' = 'stars@rNi0oG8xHe6yeeOIplm',\n" +
                "   'sink.buffer-flush.max-rows' = '100000',\n" +
                "   'sink.buffer-flush.max-bytes' = '89128960',\n" +
                "   'sink.buffer-flush.interval-ms' = '5000',\n" +
                "   'sink.properties.format' = 'json',\n" +
                "   'sink.properties.strip_outer_array' = 'true',\n" +
                "   'sink.max-retries' = '3'\n" +
                ")");

//        Table table = streamTableEnvironment.sqlQuery("select * from rule_group_mysql");
//        streamTableEnvironment.toRetractStream(table, Row.class).print();
//        env.execute();

        streamTableEnvironment.executeSql("insert into rule_group_sr select * from rule_group_mysql");
    }
}
