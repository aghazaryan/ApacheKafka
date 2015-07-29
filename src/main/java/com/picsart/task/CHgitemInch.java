package com.picsart.task;

import com.mongodb.BasicDBObject;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by aghasighazaryan on 7/27/15.
 */
public class CHgitemInch {

    private String groupId;
    private String quorum;
    private String topicName;
    private int threadNumbers;
    private ExecutorService executor;
    private ConsumerConnector consumer;

    public CHgitemInch(String groupId, String quorum, String topicName, int threadNumbers) {

        try {
            if (groupId.length() == 0 || quorum.length() == 0 || topicName.length() == 0 || threadNumbers < 1) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        this.groupId = groupId;
        this.quorum = quorum;
        this.topicName = topicName;
        this.threadNumbers = threadNumbers;

        Properties props = new Properties();
        props.put("zookeeper.connect", quorum);
        props.put("group.id", groupId);
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("consumer.timeout.ms", "1000");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    }


    public List<BasicDBObject> getMessages(int batchSize) {


        List<BasicDBObject> ret = new ArrayList<BasicDBObject>(batchSize);

        //geting topic partition count

        ZkClient zkClient = new ZkClient(quorum, 6000, 6000, new ZkSerializer() {
            public byte[] serialize(Object o) throws ZkMarshallingError {
                return ZKStringSerializer.serialize(o);
            }

            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                return ZKStringSerializer.deserialize(bytes);
            }
        });
        TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
        int patritionCount = topicMetadata.partitionsMetadata().size();

        threadNumbers = threadNumbers < patritionCount ? threadNumbers : patritionCount;

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topicName, threadNumbers);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topicName);

        executor = Executors.newFixedThreadPool(threadNumbers);
        ArrayList<Future<List<BasicDBObject>>> results = new ArrayList<Future<List<BasicDBObject>>>();

        for (KafkaStream<byte[], byte[]> stream : streams) {
            results.add(executor.submit(new Consuming(stream, batchSize)));
        }

        for (Future<List<BasicDBObject>> fs : results)
            try {
                // get() blocks until completion:
                ret.addAll(fs.get());
                TimeUnit.MILLISECONDS.sleep(3000);
            } catch (Exception e) {
                System.out.println(e);
            } finally {
                shutdown();
            }


        return ret;
    }


    private void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
}
