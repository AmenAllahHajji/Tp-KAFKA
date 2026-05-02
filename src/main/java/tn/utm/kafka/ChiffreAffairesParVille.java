package tn.utm.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {

    private static final String TOPIC            = "pos-events";
    private static final String GROUP_ID         = "ca-1";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final Map<String, Double> caParVille = new HashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static long dernierAffichage = System.currentTimeMillis();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,       "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("=== ChiffreAffairesParVille démarré (groupe: " + GROUP_ID + ") ===");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt...");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode json  = mapper.readTree(record.value());
                        String ville   = json.get("ville").asText();
                        String type    = json.get("type").asText();
                        double montant = json.has("montant") ? json.get("montant").asDouble() : 0;

                        if (type.equals("VENTE")) {
                            caParVille.merge(ville, montant, Double::sum);
                        } else if (type.equals("RETOUR")) {
                            caParVille.merge(ville, -montant, Double::sum);
                        }
                        // OUVERTURE ignorée pour le CA

                    } catch (Exception e) {
                        System.err.println("Erreur parsing JSON : " + e.getMessage());
                    }
                }

                // Commit manuel après chaque batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                // Afficher toutes les 5 secondes
                long now = System.currentTimeMillis();
                if (now - dernierAffichage >= 5000) {
                    afficherCA();
                    dernierAffichage = now;
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("Consumer interrompu proprement.");
        } finally {
            consumer.close();
            System.out.println("ChiffreAffairesParVille arrêté.");
        }
    }

    private static void afficherCA() {
        System.out.println("\n======= CHIFFRE D'AFFAIRES PAR VILLE =======");
        if (caParVille.isEmpty()) {
            System.out.println("  (aucune donnée pour l'instant)");
        } else {
            caParVille.forEach((ville, ca) ->
                System.out.printf("  %-10s : %.2f DT%n", ville, ca));
        }
        System.out.println("============================================\n");
    }
}