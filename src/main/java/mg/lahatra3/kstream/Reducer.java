package mg.lahatra3.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class Reducer {

   public static void main(String[] args) throws FileNotFoundException, IOException {
      Properties config = new Properties();
      String filenamePathStr = "src/main/resources/kstreams.properties";

      try (FileInputStream fileInputStream = new FileInputStream(filenamePathStr)) {
         config.load(fileInputStream);
      }
      config.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-stream-1");
      config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

      final StreamsBuilder streamsBuilder = new StreamsBuilder();

      KStream<String, String> streamPurchase = streamsBuilder.stream("input-purchase");

      streamPurchase
          .groupByKey()
          .reduce(
              (a, b) -> String.valueOf(Integer.parseInt(a) + Integer.parseInt(b)),
              Materialized.with(Serdes.String(), Serdes.String())
          )
          .toStream()
          .to("total-purchase");

      KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
      kafkaStreams.start();

      try {
         Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }
}
