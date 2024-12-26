package nfad.maryam.frauddetection.db;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.time.Instant;

public class InfluxDBConnector {
    private final InfluxDBClient influxDBClient;
    private final String bucket = "fraud_detection";
    private final String org = "myorg";

    public InfluxDBConnector() {
        String token = "my-super-secret-token";
        String url = "http://localhost:8086";

        this.influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
    }
    public void saveTransaction(String userId, double amount, String timestamp) {
        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            // Enregistrement de la transaction dans un seul point avec plusieurs champs
            Point point = Point
                    .measurement("suspicious_transactions")  // La mesure (nom de la table)
                    .addTag("userId", userId)  // Champ pour l'identifiant de l'utilisateur
                    .addField("amount", amount)  // Champ pour le montant de la transaction
                    .time(Instant.parse(timestamp), WritePrecision.NS);  // Le timestamp

            writeApi.writePoint(point);  // Écriture du point dans InfluxDB
        } catch (Exception e) {
            System.err.println("Erreur lors de l'écriture dans InfluxDB: " + e.getMessage());
        }
    }

    public void close() {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}