import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class Test {
    @org.junit.Test
    public void test(){
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.49.0.86")
                .port(3306)
                .username("bdp")
                .password("akd_bdp")
                .databaseList("dev_erp")
                .tableList("dev_erp.order_item_mws_bak")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        System.out.println(mySqlSource instanceof ResultTypeQueryable);
    }
}
