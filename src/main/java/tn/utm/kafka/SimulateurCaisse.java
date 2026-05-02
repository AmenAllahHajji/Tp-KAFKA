package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class SimulateurCaisse {

    private static final String TOPIC            = "pos-events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String[] VILLES = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabès"};
    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     "true");
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("=== SimulateurCaisse démarré ===");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt du SimulateurCaisse...");
            producer.close();
        }));

        while (true) {
            // Choisir ville aléatoire → clé = ville (même partition)
            String ville = VILLES[random.nextInt(VILLES.length)];

            // Type selon probabilités : VENTE 70%, RETOUR 10%, OUVERTURE 20%
            String type;
            int rand = random.nextInt(100);
            if      (rand < 70) type = "VENTE";
            else if (rand < 80) type = "RETOUR";
            else                type = "OUVERTURE";

            // Montant entre 5 et 500 DT (0 pour OUVERTURE)
            double montant = 0;
            if (!type.equals("OUVERTURE")) {
                montant = 5 + (random.nextDouble() * 495);
            }

            // Créer l'événement et sérialiser en JSON
            EvenementCaisse evenement = new EvenementCaisse(ville, type, montant);
            String jsonValue = evenement.toJson();

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, ville, jsonValue);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Envoyé -> " + jsonValue
                            + " | partition=" + metadata.partition()
                            + " | offset="    + metadata.offset());
                } else {
                    System.err.println("Erreur : " + exception.getMessage());
                }
            });

            // Attendre entre 100 et 500 ms
            Thread.sleep(100 + random.nextInt(401));
        }
    }
}