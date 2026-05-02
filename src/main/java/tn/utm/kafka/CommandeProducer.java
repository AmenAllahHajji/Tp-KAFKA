package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Arrays;
import java.util.Properties;

public class CommandeProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        ObjectMapper mapper = new ObjectMapper();

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                Commande cmd = new Commande(
                    "CMD-00" + i,
                    "2026-05-02",
                    Arrays.asList("Article-A", "Article-B"),
                    100.0 * i
                );

                String json = mapper.writeValueAsString(cmd);

                ProducerRecord<String, String> record =
                    new ProducerRecord<>("commandes", cmd.getId(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erreur : " + exception.getMessage());
                    } else {
                        System.out.printf(
                            "✓ Envoyé — partition=%d, offset=%d%n",
                            metadata.partition(), metadata.offset()
                        );
                    }
                });

                System.out.println("JSON envoyé : " + json);
            }
            producer.flush();
        }
    }
}