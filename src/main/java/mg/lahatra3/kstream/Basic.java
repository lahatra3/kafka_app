package mg.lahatra3.kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;

public class Basic {
   public static final String INPUT_TOPIC = "string-input";
   public static final String OUTPUT_TOPIC = "string-output";

   public static void main(String[] args) {
      Properties config = new Properties();
      config.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
      config.put("bootstrap.servers", "localhost:9094");
      config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

      final StreamsBuilder streamsBuilder = new StreamsBuilder();
      KStream<String, String> source = streamsBuilder.stream(INPUT_TOPIC);

//      source = source.filter((key, value) -> value.length() > 5);
//      source = source.mapValues((value) -> value.toUpperCase());

//      source = source.flatMap((key, value) -> {
//         String[] tokens =  value.split(" ");
//         return Arrays.stream(tokens)
//             .map(s -> new KeyValue<>(key, s))
//             .toList();
//      });

//      source.foreach((key, value) -> {
//         System.out.printf("Message: %s\n", value);
//      });

      source.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

      KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
      kafkaStreams.start();

      try {
         Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }
}
