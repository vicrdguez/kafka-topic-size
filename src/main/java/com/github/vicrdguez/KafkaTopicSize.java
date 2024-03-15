package com.github.vicrdguez;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "kafka-topic-size", mixinStandardHelpOptions = true, version = "0.1", description = "Calculates size of provided topics by reading its contents")
public class KafkaTopicSize implements Callable<Integer> {

    @Option(names = { "--bootstrap", "-b" }, description = "Kafka bootstrap servers")
    private String bootstrapServers;

    @Option(names = { "--config", "-c" }, description = "Kafka client properties file")
    private File configFile;

    @Option(names = { "--from-time",
            "-t" }, description = "Time back from the current moment from were to calculate the size (hours, days)")
    private String fromTime;

    @Parameters(index = "0", description = "Topic name, or Regex pattern used to identify topics to calculate the size from")
    private String pattern;

    @Override
    public Integer call() throws Exception {
        Properties props = new Properties();
        if (configFile != null) {
            props.load(new FileInputStream(configFile));
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        SizeConsumer sizeConsumer = new SizeConsumer(props, pattern, parseTimeFrom(fromTime));
        sizeConsumer.run();
        return 0;
    }

    private long parseTimeFrom(String fromTimeStr) {
        Long time = Long.valueOf(fromTimeStr.substring(0, fromTimeStr.length() - 1));
        if (fromTimeStr.endsWith("h")) {
            return Duration.ofHours(time).toMillis();
        }
        if (fromTimeStr.endsWith("d")) {
            return Duration.ofDays(time).toMillis();
        }

        throw new IllegalArgumentException(
                "The option --from-time only supports days (d) and hours (h)");
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new KafkaTopicSize()).execute(args);
        System.exit(exitCode);
    }
}
