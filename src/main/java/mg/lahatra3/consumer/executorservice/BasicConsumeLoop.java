package mg.lahatra3.consumer.executorservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BasicConsumeLoop<K, V> implements Runnable {
   private final KafkaConsumer<K, V> consumer;
   private final List<String> topics;
   private final AtomicBoolean shutdown;

   public BasicConsumeLoop(Map<String, Object> config, List<String> topics) {
      this.consumer = new KafkaConsumer<K, V>(config);
      this.topics = topics;
      this.shutdown = new AtomicBoolean(false);
   }

   public abstract void process(ConsumerRecord<K, V> record);

   @Override
   public void run() {
      try {
         consumer.subscribe(topics);

         while (!shutdown.get()) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(this::process);
         }
      } finally {
         consumer.close();
      }
   }
}
