package nfad.maryam.frauddetection;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import nfad.maryam.frauddetection.db.InfluxDBConnector;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FraudDetectionApp {
    private static final String INPUT_TOPIC = "transactions-input";
    private static final String OUTPUT_TOPIC = "fraud-alerts";
    private static final double SUSPICIOUS_AMOUNT = 10000.0;

    public static void main(String[] args) {
        // Configuration Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Création du builder de topologie
        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();
        InfluxDBConnector influxDBConnector = new InfluxDBConnector();

        // Création du stream depuis le topic d'entrée
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // Traitement des transactions
        KStream<String, String> fraudAlerts = inputStream
                .filter((key, value) -> {
                    try {
                        JsonNode transaction = mapper.readTree(value);
                        double amount = transaction.get("amount").asDouble();
                        return amount > SUSPICIOUS_AMOUNT;
                    } catch (Exception e) {
                        System.err.println("Erreur lors du parsing de la transaction: " + e.getMessage());
                        return false;
                    }
                })
                .peek((key, value) -> {
                    try {
                        // Sauvegarde dans InfluxDB
                        JsonNode transaction = mapper.readTree(value);
                        influxDBConnector.saveTransaction(
                                transaction.get("userId").asText(),
                                transaction.get("amount").asDouble(),
                                transaction.get("timestamp").asText()
                        );
                    } catch (Exception e) {
                        System.err.println("Erreur lors de la sauvegarde dans InfluxDB: " + e.getMessage());
                    }
                });

        // Publication des alertes dans le topic de sortie
        fraudAlerts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Démarrage de l'application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajout d'un hook pour l'arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}