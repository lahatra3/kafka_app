package mg.lahatra3.consumer.executorservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaAppConsumerMultiThread {

   private static final ExecutorService executorService = Executors.newFixedThreadPool(3);

   public static void main(String[] args) {
      Map<String, Object> config = getConfig();

      BasicConsumeLoop<String, String> consumer1 = new BasicConsumeLoop<String, String>(config, List.of("my-topic-multi-thread")) {
         @Override
         public void process(ConsumerRecord<String, String> record) {
            System.out.printf("Consumer 1 - offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
         }
      };

      BasicConsumeLoop<String, String> consumer2 = new BasicConsumeLoop<String, String>(config, List.of("my-topic-multi-thread")) {
         @Override
         public void process(ConsumerRecord<String, String> record) {
            System.out.printf("Consumer 2 - offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
         }
      };

      BasicConsumeLoop<String, String> consumer3 = new BasicConsumeLoop<String, String>(config, List.of("my-topic-multi-thread")) {
         @Override
         public void process(ConsumerRecord<String, String> record) {
            System.out.printf("Consumer 3 - offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
         }
      };

      executorService.execute(consumer1);
      executorService.execute(consumer2);
      executorService.execute(consumer3);
   }

   private static Map<String, Object> getConfig() {
      return Map.ofEntries(
          Map.entry("bootstrap.servers", "localhost:9094"),
          Map.entry("group.id", "lahatra3_group"),
          Map.entry("enable.auto.commit", "true"),
          Map.entry("auto.commit.interval.ms", "1000"),
          Map.entry("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
          Map.entry("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      );
   }
}
