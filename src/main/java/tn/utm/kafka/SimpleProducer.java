package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 1000; i++) {
                String key = "client-" + (i % 3);
                String value = "Achat numéro " + i + " (" + (50 + i * 17) + " DT)";

                ProducerRecord<String, String> record =
                    new ProducerRecord<>("ventes", key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Échec d'envoi : " + exception.getMessage());
                    } else {
                        System.out.printf(
                            "✓ Envoyé — partition=%d, offset=%d, key=%s%n",
                            metadata.partition(), metadata.offset(), key
                        );
                    }
                });
            }
            producer.flush();
        }
    }
}