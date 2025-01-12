import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Locale;



import java.util.Properties;

public class UppercaseStreamApp {
    public static void main(String[] args) {
        // Configuration des propriétés Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "uppercase-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Construire le flux
        StreamsBuilder builder = new StreamsBuilder();

        // Lire depuis le topic source
        KStream<String, String> sourceStream = builder.stream("input-topic");

        // Transformer les messages en majuscules


        KStream<String, String> uppercasedStream = sourceStream.mapValues(value -> value.toUpperCase(Locale.ROOT));


        // Écrire dans le topic cible
        uppercasedStream.to("output-topic");

        // Construire et démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Ajouter un gestionnaire pour une fermeture propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}
