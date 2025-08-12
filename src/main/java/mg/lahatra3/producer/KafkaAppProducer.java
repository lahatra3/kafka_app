package mg.lahatra3.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class KafkaAppProducer {

   public static void main(String[] args) {
      Map<String, Object> config = Map.ofEntries(
          Map.entry("bootstrap.servers", "localhost:9094"),
          Map.entry("group.id", "lahatra3_group"),
          Map.entry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
          Map.entry("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
          Map.entry("acks", "all")
      );

      KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);

      for (int i  = 0; i < 100; i++) {
         producer.send(
             new ProducerRecord<String, String>(
                 "my-topic-multi-thread",
                 Integer.toString(i),
                 Integer.toString(i)
             )
         );
      }
      producer.close();
   }
}
