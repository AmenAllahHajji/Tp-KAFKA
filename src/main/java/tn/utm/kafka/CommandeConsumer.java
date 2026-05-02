package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CommandeConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupe-commandes");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ObjectMapper mapper = new ObjectMapper();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("commandes"));

            System.out.println("⏳ En attente de commandes... (Ctrl+C pour arrêter)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    Commande cmd = mapper.readValue(record.value(), Commande.class);
                    System.out.println("================================");
                    System.out.println("▶ ID       : " + cmd.getId());
                    System.out.println("▶ Date     : " + cmd.getDate());
                    System.out.println("▶ Articles : " + cmd.getArticles());
                    System.out.println("▶ Total    : " + cmd.getTotal() + " DT");
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}