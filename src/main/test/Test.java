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

    @org.junit.Test
    public void test1() {
        String s = "lbw";
        int s2 = 0;

        String check = check(s);
        String check1 = check(null);
        Integer check2 = check(s2);

        System.out.println(check2);
    }

    public <T> T check(T value) {
        if (null == value) {
            return (T) "value is null";
        } else {
            return value;
        }
    }
}
