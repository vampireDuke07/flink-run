package com.mq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.mq.PulsarConstant.ADMIN_SERVICE_URL;
import static com.mq.PulsarConstant.SERVICE_URL;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;

public class PulsarPoc {
    public static final Pattern SOME_TOPICS_IN_NAMESPACE =
            // Pattern.compile("persistent://wangminchao/ad/reports_.*");
            // Pattern.compile("persistent://wangminchao/mutilPartition/reports_[5-9][0-9]*");
    Pattern.compile("persistent://wangminchao/mutilPartition/reports_.*");
    // Pattern.compile("persistent://public/default/r.*");
    public static String regexTopic = "persistent://wangminchao/mutilPartition/reports_.*";
    public static String topicName = "persistent://wangminchao/mutilPartition/reports_%s";
    public static String subscriptionName = "sub_reports";
    static ExecutorService producerExecutor = Executors.newFixedThreadPool(1000);
    static ExecutorService consumerExecutor = Executors.newFixedThreadPool(1000);

    public static void main(String[] args) throws Exception {
        PulsarAdmin admin = getPulsarAdmin();
        PulsarClient client = getPulsarClient();
        String topicNamePrefix = "persistent://wangminchao/mutilPartition/reports_%s";
        // deleteNamespace(admin, "wangminchao/ad");
        // List<String> topicList = listPulsarTopic(admin, "wangminchao/ad");
        // Stream<String> topicStream = topicList.parallelStream();
        final String reportsMsg = readFile();
        /*  int retentionTime = 3 * 24 * 60; // 3days
        int retentionSize = 10 * 1024; // 10G
        RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
        admin.namespaces().setRetention("wangminchao/ad", policies);
        System.out.println(admin.namespaces().getRetention("wangminchao/ad"));*/
        // createNamespace(admin, "wangminchao/mutilPartition");

        // for (int i = 0; i < 10000; i++) {
        //     createPulsarTopic(
        //             admin,
        //             String.format(topicNamePrefix, i),
        //             1000);
        // }

        for (int i = 0; i < 100; i++) {
            int finalI = i;
            producerExecutor.submit(
                    () -> {
                        Producer<byte[]> producer =
                                client.newProducer()
                                        .topic(topicName)
                                        .enableBatching(true)
                                        // .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                                        .sendTimeout(100, TimeUnit.SECONDS)
                                        .blockIfQueueFull(true)
                                        .compressionType(CompressionType.LZ4)
                                        .property("initialSubscriptionName", subscriptionName)
                                        .create();
                        while (true) {
                            producePulsarMessages(
                                    producer,
                                    reportsMsg,
                                    String.format(
                                            topicNamePrefix,
                                            1));
                        }
                    });
        }

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            ConsumerBuilder consumerBuilder =
                    client.newConsumer()
                            .subscriptionType(SubscriptionType.Shared)
                            .subscriptionName(subscriptionName);
            consumerExecutor.submit(
                    () -> {
                        consumePulsarMessages(consumerBuilder, String.format(
                                topicNamePrefix, 1));
                    });
        }
        // for (int i = 0; i < 100; i++) {
        // 提交100个线程去写，每个线程轮询写10w个topic
        /*producerExecutor.submit(
        () -> {
            while (true) {
                // createNamespace(admin, namespace);
                // createPulsarTopic(admin, String.format(topicNamePrefix, i, i), 1);
                try {
                    producePulsarMessages(
                            client,
                            reportsFile,
                            String.format(
                                    topicNamePrefix, new Random().nextInt(10 * 10000)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });*/

        /*producerExecutor.submit(
        () -> {
            while (true) {
                producePulsarMessages(
                        client,
                        reportsMsg,
                        String.format(
                                topicNamePrefix, new Random().nextInt(10 * 1000) + 1));
            }
        });*/
        /*while (true) {
            producePulsarMessages(
                    client,
                    reportsMsg,
                    String.format(
                            topicNamePrefix, new Random().nextInt(10 * 1000) + 1));
        }*/

        /*consumerExecutor.submit(
        () -> {
            while (true) {
                consumePulsarMessages(client);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });*/
        /*consumerExecutor.submit(
        () -> {
            for (int y = 1; y < 10; y++) {
                try {
                    consumePulsarMessages(
                            client, Pattern.compile(String.format(regexTopics, y)));
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            }
        });*/
        // System.out.println("loop num :" + i);
        // }
        // pulsar.consumePulsarMessages(client);
        // client.close();
        // admin.close();
        // producerExecutor.shutdown();
        // consumerExecutor.shutdown();
    }

    public static void createPulsarTopic(PulsarAdmin admin, String topicName, int numPartitions) {
        try {
            if (numPartitions > 1) {
                admin.topics().createNonPartitionedTopic(topicName);
            } else {
                admin.topics().createPartitionedTopic(topicName, numPartitions);
            }
            System.out.println("Successfully created topicName: " + topicName);
        } catch (PulsarAdminException e) {
            System.err.println("Unsuccessfully created topicName: " + topicName);
        }
    }

    public static List<String> listPulsarTopic(PulsarAdmin admin, String namespaces) {
        try {
            List<String> topicList = admin.topics().getList(namespaces);
            System.out.println("Namespaces topic size: " + topicList.size());
            return topicList;
        } catch (PulsarAdminException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void deletePulsarTopic(PulsarAdmin admin, String topicName) {
        try {
            admin.topics().delete(topicName, true);
            // System.out.println("Successfully deleted topicName: " + topicName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*admin.topics()
        .deletePartitionedTopicAsync(topicName, true)
        .thenAccept(
                new java.util.function.Consumer<Void>() {
                    @Override
                    public void accept(Void unused) {
                        System.out.println("Successfully deleted topicName: " + topicName);
                    }
                })
        .exceptionally(
                new Function<Throwable, Void>() {
                    @Override
                    public Void apply(Throwable throwable) {
                        System.err.println(
                                "Unsuccessfully deleted topicName: " + topicName + "," + throwable.getMessage());
                        return null;
                    }
                });*/
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

    public static PulsarAdmin getPulsarAdmin() throws PulsarClientException {
        boolean useTls = false;
        boolean tlsAllowInsecureConnection = false;
        String tlsTrustCertsFilePath = null;
        return PulsarAdmin.builder()
                .authentication(AuthenticationFactory.token(PulsarConstant.AUTH_PARAMS))
                .serviceHttpUrl(ADMIN_SERVICE_URL)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .build();
    }

    public static void createNamespace(PulsarAdmin admin, String namespace)
            throws PulsarAdminException {
        admin.namespaces().createNamespace(namespace);
    }

    public static void deleteNamespace(PulsarAdmin admin, String namespace)
            throws PulsarAdminException {
        admin.namespaces().deleteNamespace(namespace, true);
    }

    private static String readFile() throws IOException {
        Path path =
                Paths.get(
                        "/app/tmp/pulsar/src/main/resources/reports.txt");
        // byte[] bytes = Files.readAllBytes(path);
        List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_16);
        StringBuilder sb = new StringBuilder();
        for (String line : allLines) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public static void producePulsarMessages(
            Producer<byte[]> producer, String message, String topicName) {
        try {
            producer.newMessage()
                                .key("key_business_" + new Random().nextInt(10))
                                .value(message.getBytes())
                                .sendAsync()
                                .thenAccept(
                                        (messageId -> {
                                            // System.out.println(
                                            //         "topicName: " + topicName + ", messageId: " + messageId);
                                        }))
                                .exceptionally(
                                        new Function<Throwable, Void>() {
                                            @Override
                                            public Void apply(Throwable throwable) {
                                                throwable.printStackTrace();
                                                return null;
                                            }
                                        });
            // System.out.println(topicName);
            /*producer.newMessage()
                    .key("key_business_" + new Random().nextInt(10))
                    .value(message.getBytes())
                    .send();*/

            // producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // producer.close();
    }

    public static void consumePulsarMessages(ConsumerBuilder consumerBuilder, String topicName) {

        try {

            // Subscribe to a subsets of topics in a namespace, based on regex
            // 1
            /*consumerBuilder
            .topicsPattern(SOME_TOPICS_IN_NAMESPACE)
            .subscriptionTopicsMode(RegexSubscriptionMode.PersistentOnly)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()
            .receiveAsync()
            .thenAccept(
                    message -> {
                        System.out.println(message);
                        MessageImpl msg = (MessageImpl) message;
                        System.out.println(msg.getData());
                    });*/
            // 2
            consumerBuilder
                    .topic(topicName)
                    // .topicsPattern(SOME_TOPICS_IN_NAMESPACE)
                    .subscriptionTopicsMode(RegexSubscriptionMode.PersistentOnly)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribeAsync()
                    .thenAccept(PulsarPoc::receiveMessageFromConsumer);
            // 3
            /*Consumer consumer =
                    consumerBuilder
                            .topic(topicName)
                            .subscriptionTopicsMode(RegexSubscriptionMode.PersistentOnly)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscribe();
            while (true) {
                // Wait for a message
                Messages msgs = consumer.batchReceive();
                if (msgs.size() > 0) {
                    // Do something with the message
                    for (Object msg : msgs) {
                        // do something
                        Message m = (Message ) msg;
                        // System.out.println(
                        //         "Message received: " + m.getTopicName() + ", " + m.getMessageId());
                        // Acknowledge the message so that it can be deleted by the message broker
                    }
                    // System.out.println(  topicName + ", " + msgs.size());
                    consumer.acknowledge(msgs);
                }
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void receiveMessageFromConsumer(Object consumer) {
        ((Consumer) consumer)
                .receiveAsync()
                .thenAccept(
                        message -> {
                            // Do something with the received message
                            // org.apache.pulsar.client.impl.TopicMessageImpl@52bb6c7c
                            MessageImpl msg = (MessageImpl) message;
                            System.out.println(msg.getTopicName() + "," + msg.getMessageId());
                            receiveMessageFromConsumer(consumer);
                        })
                .exceptionally(
                        o -> {
                            System.out.println(o);
                            /*try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }*/
                            return null;
                        });
    }
}
