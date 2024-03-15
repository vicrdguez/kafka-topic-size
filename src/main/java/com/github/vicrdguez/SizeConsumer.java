package com.github.vicrdguez;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class SizeConsumer {
    private KafkaConsumer<byte[], byte[]> consumer;
    private final Long from;
    private final Pattern topicPattern;
    private static final Duration timeout = Duration.ofMillis(100);
    private static AtomicBoolean stop = new AtomicBoolean(false);
    private final Map<Integer, Long> lastTsSeen;
    private Long now;

    public SizeConsumer(Properties properties, String pattern, Long fromTime) {
        Properties props = properties;
        // we are not interested in the content, just the size of it
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-topic-size-" + UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        this.topicPattern = Pattern.compile(pattern);
        this.now = System.currentTimeMillis();
        this.from = now - fromTime;
        this.consumer = new KafkaConsumer<>(props);
        this.lastTsSeen = new HashMap<>();
    }

    public void run() throws InterruptedException {
        consumer.subscribe(topicPattern);
        // getting the assignment
        Set<TopicPartition> assignment = new HashSet<>();
        // initial poll just to get the assignment, larger timeout since the consumer is
        // still subscribing
        consumer.poll(Duration.ofSeconds(1));
        assignment = consumer.assignment();
        // getting the offsets for each partition on the timestamp defined in `from`
        Map<TopicPartition, Long> partitionTsMap = assignment
                .stream()
                .collect(Collectors.toMap(tp -> tp, tp -> from));
        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(partitionTsMap);
        // reset the consumer to said offsets
        partitionOffsetMap.forEach((tp, ot) -> {
            if (ot == null) {
                System.out.printf("No offset found in defined time fame in %s-%s\n",
                        tp.topic(), tp.partition());
                return;
            }
            consumer.seek(tp, ot.offset());
        });

        Stats stats = new Stats();
        stats.report();
        while (!stop.get()) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                lastTsSeen.put(rec.partition(), rec.timestamp());
                stats.updateForTopic(rec.topic(), rec.partition(), recordSize(rec));
            }

            stop.set(lastTsSeen.values()
                    .stream()
                    .allMatch(ts -> ts >= now) || records.isEmpty());
        }
        // give some time for all topics to be reported on screen
        Thread.sleep(5000);
        stats.printResults();
        System.exit(0);
    }

    private Integer recordSize(ConsumerRecord<byte[], byte[]> rec) {
        Integer size = 0;
        if (rec.serializedKeySize() != -1)
            size += rec.serializedKeySize();
        if (rec.serializedValueSize() != -1)
            size += rec.serializedValueSize();

        return size;
    }
}
