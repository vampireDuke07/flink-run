import common.PulsarConstant;
import org.apache.pulsar.client.api.*;

import static common.PulsarConstant.SERVICE_URL;
import mockit.Mocked;

/**
 * @author liangjianxiang
 * @date 2022/6/20 16:30
 */
public class PulsarProducer {
    public static void main(String[] args) throws PulsarClientException {


        PulsarClient client = getPulsarClient();

        //pulsar 生产者
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("SR_test")
                .create();

        //向pulsar发送消息
        producer.send("{\"id\": 2, \"author\": \"ljx2\", \"title\": 892, \"price\": 22.08}");

        producer.close();
        client.close();

    }

    public static PulsarClient getPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .authentication(AuthenticationFactory.token(PulsarConstant.AUTH_PARAMS))
                // .proxyServiceUrl("pulsar+ssl://42.194.172.174:6651", ProxyProtocol.SNI)
                .serviceUrl(SERVICE_URL)
                .memoryLimit(1, SizeUnit.GIGA_BYTES)
                .ioThreads(4)
                // .maxConcurrentLookupRequests()
                .build();
    }

}
