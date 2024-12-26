package nfad.maryam.frauddetection.test;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionGenerator {
    private static final String TOPIC = "transactions-input";
    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        // Configuration du producteur Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) { // Boucle infinie pour générer des transactions
                String transaction = generateTransaction();
                producer.send(new ProducerRecord<>(TOPIC, transaction));
                System.out.println("Transaction envoyée: " + transaction);
                Thread.sleep(1000); // Attendre 1 seconde entre chaque transaction
            }
        } catch (Exception e) {
            System.err.println("Erreur lors de la génération des transactions: " + e.getMessage());
        }
    }

    private static String generateTransaction() throws Exception {
        // Générer un identifiant utilisateur aléatoire entre 1 et 100
        String userId = String.format("USER_%03d", RANDOM.nextInt(10) + 1);

        // Générer un montant aléatoire entre 100 et 15000
        double amount = 100 + RANDOM.nextDouble() * 14900;

        // Créer l'objet transaction
        Transaction transaction = new Transaction(
                userId,
                amount,
                Instant.now().toString()
        );

        // Convertir en JSON
        return MAPPER.writeValueAsString(transaction);
    }

    private static class Transaction {
        public String userId;
        public double amount;
        public String timestamp;

        public Transaction(String userId, double amount, String timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }
}