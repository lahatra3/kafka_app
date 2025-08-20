package mg.lahatra3.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class Peek {
   public static void main(String[] args) throws IOException, FileNotFoundException {
      Properties config = new Properties();
      String filenamePathStr = "src/main/resources/kstreams.properties";

      try (FileInputStream fileInputStream = new FileInputStream(filenamePathStr)) {
         config.load(fileInputStream);
      }
      config.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());

      final String INPUT_TOPIC = config.getProperty("input.topic");
      final String OUTPUT_TOPIC = config.getProperty("output.topic");

      final StreamsBuilder streamsBuilder = new StreamsBuilder();
      KStream<String, String> sources = streamsBuilder.stream(INPUT_TOPIC);

      sources.peek((key, value) -> {
         System.out.printf("key %s : value %s\n", key, value);
      });

      sources.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

      KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
      kafkaStreams.start();

      try {
         Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }
}
