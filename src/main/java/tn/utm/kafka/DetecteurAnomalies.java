package tn.utm.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class DetecteurAnomalies {

    private static final String TOPIC_SOURCE     = "pos-events";
    private static final String TOPIC_ALERTES    = "alertes-retours";
    private static final String GROUP_ID         = "alerte-1";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        // === CONSUMER ===
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,                 GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        // === PRODUCER (alertes) ===
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG,                   "all");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Collections.singletonList(TOPIC_SOURCE));

        System.out.println("=== DetecteurAnomalies démarré (groupe: " + GROUP_ID + ") ===");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt du DetecteurAnomalies...");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode json  = mapper.readTree(record.value());
                        String type    = json.get("type").asText();
                        String ville   = json.get("ville").asText();
                        double montant = json.has("montant") ? json.get("montant").asDouble() : 0;

                        System.out.println("Reçu -> " + record.value());

                        // Détecter RETOUR > 200 DT
                        if (type.equals("RETOUR") && montant > 200.0) {

                            String alerte = String.format(
                                "{\"alerte\":\"RETOUR_SUSPECT\",\"ville\":\"%s\",\"montant\":%.2f,\"source\":\"%s\"}",
                                ville, montant, json.get("idCaisse").asText()
                            );

                            ProducerRecord<String, String> alerteRecord =
                                    new ProducerRecord<>(TOPIC_ALERTES, ville, alerte);

                            producer.send(alerteRecord, (metadata, exception) -> {
                                if (exception == null) {
                                    System.out.println(">>> ALERTE envoyée : " + alerte);
                                } else {
                                    System.err.println("Erreur envoi alerte : " + exception.getMessage());
                                }
                            });
                        }

                    } catch (Exception e) {
                        System.err.println("Erreur parsing JSON : " + e.getMessage());
                    }
                }

                // Commit manuel après chaque batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("Consumer interrompu proprement.");
        } finally {
            consumer.close();
            producer.close();
            System.out.println("DetecteurAnomalies arrêté.");
        }
    }
}