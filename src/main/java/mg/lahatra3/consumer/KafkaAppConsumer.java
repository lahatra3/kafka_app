package mg.lahatra3.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class KafkaAppConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = getConsumer();
        consumer.subscribe(List.of("my-topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }

    private static KafkaConsumer<String, String> getConsumer() {
        Map<String, Object> config = Map.ofEntries(
           Map.entry("bootstrap.servers", "localhost:9094"),
           Map.entry("group.id", "lahatra3_group"),
           Map.entry("enable.auto.commit", "true"),
           Map.entry("auto.commit.interval.ms", "1000"),
           Map.entry("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
           Map.entry("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        );

       return new KafkaConsumer<String, String>(config);
    }
}
