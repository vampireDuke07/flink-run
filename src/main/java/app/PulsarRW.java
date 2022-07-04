package app;

import common.PulsarConstant;
import org.apache.pulsar.client.api.*;

import static common.PulsarConstant.SERVICE_URL;

/**
 * @author liangjianxiang
 * @date 2022/6/20 14:50
 */
public class PulsarRW {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = getPulsarClient();

        producer(client);

//        consumer(client);

        client.close();
    }

    public static PulsarClient getPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .authentication(AuthenticationFactory.token(PulsarConstant.AUTH_PARAMS))
//                 .proxyServiceUrl("pulsar+ssl://42.194.172.174:6651", ProxyProtocol.SNI)
                .serviceUrl(SERVICE_URL)
                .memoryLimit(1, SizeUnit.GIGA_BYTES)
                .ioThreads(4)
                // .maxConcurrentLookupRequests()
                .build();
    }

    public static void producer(PulsarClient client) throws PulsarClientException {
        //pulsar 生产者
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("sr_dev")
                .create();

        //向pulsar发送消息
        producer.send("{\"id\": 13, \"author\": \"ljx14\", \"title\": 8915, \"price\": 207.16, \"ts\": 20220717}");
    }

    public static void consumer(PulsarClient client) throws PulsarClientException {
        //pulsar 消费者
        Consumer<byte[]> consumer = client.newConsumer()
                .topic("sr_dev")
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        //从pulsar消费数据
        while (true) {
            // Wait for a message
            Message msg = consumer.receive();

            try {
                // Do something with the message
                System.out.println("Message received: " + new String(msg.getData()));

                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }

}
