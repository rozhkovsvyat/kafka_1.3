package com.work.consumers;

import com.work.dto.MessageDto;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class AvroSingleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AvroSingleConsumer.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "127.0.0.1:9094");
        String schemaRegistry = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://127.0.0.1:8081");
        String groupId = System.getenv().getOrDefault("GROUP_ID", "single-group");
        String topic = System.getenv().getOrDefault("TOPIC", "test-topic");

        int sendingPeriodInMillis = 500; // Периодичность отправки сообщений продюсером в мс.

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Список адресов брокеров кластера Kafka.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Уникальный идентификатор группы потребителей (позволяет балансировать партиции между репликами приложения).
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Сериализатор ключа сообщения String -> byte[].
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName()); // Сериализатор значения сообщения object -> byte[] (Avro).
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry); // Адрес реестра для регистрации и проверки Avro-схем.
        properties.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true); // Включает поддержку логических типов Avro (в проекте используется для работы с UUID как с объектом Java).
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, true); // Включает генерацию Avro-схемы через Reflection (не требует .avsc-файла для MessageDto).
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // Включает автоматическое подтверждение прочитанных смещений (offsets).
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, sendingPeriodInMillis); // Периодичность автоматических подтверждений прочитанных смещений в мс.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Стратегия поведения при отсутствии смещения: "earliest" заставляет читать топик с самого начала.
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100); // Максимальное время накопления данных брокером в топике перед ответом потребителю.
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1); // Минимальное количество данных в топике в байтах, которое брокер должен накопить перед ответом потребителю.

        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties);

        // Обработка корректного завершения приложения (graceful shutdown): при выключении контейнера у потребителя вызовется wakeup() для выхода из цикла poll().
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Waiting for consumer to shut down...");
            kafkaConsumer.wakeup();
            try {
                mainThread.join(5000);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            logger.info("Single consumer started and subscribed to {}.", topic);

            while (true) {
                // Запрос новой порции данных, при отсутствии данных блокирует поток на 100 мс. 
                ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (consumerRecords.isEmpty()){
                    continue;
                }

                // Получение метаданных о партициях для определения брокера сообщения.
                List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);

                for (ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
                    try {
                        MessageDto messageDto = (MessageDto) consumerRecord.value();

                        // Определение брокера сообщения.
                        int brokerId = partitions
                            .stream()
                            .filter(streamPartition -> streamPartition.partition() == consumerRecord.partition())
                            .map(streamPartition -> streamPartition.leader().id())
                            .findFirst()
                            .orElse(-1);

                        logger.info("Received [Bytes: {}] [BrokerId: {}] [ID: {}] [Number: {}] [Topic: {}] [Partition: {}] [Offset: {}].", 
                            consumerRecord.serializedValueSize(), brokerId, messageDto.getId(), messageDto.getNumber(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());

                        // Некоторая бизнес-логика обработки сообщения.
                        processMessage(messageDto);

                    } catch (ClassCastException | SerializationException exception) {
                        logger.error("Failed to deserialize message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);

                    } catch (Exception exception) {
                        logger.error("Failed to process message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);
                    }
                }
            }
        } catch (WakeupException exception) {
            // Ожидаемое исключение при вызове kafkaConsumer.wakeup().
            logger.info("Shutting down consumer...");
        } catch (Exception exception) {
            logger.error("Unexpected subscription error.", exception);
        } finally {
            // Освобождение ресурсов и закрытие сетевых соединений.
            kafkaConsumer.close();
            logger.info("Consumer closed.");
        }
    }

    private static void processMessage(MessageDto messageDto) {
        logger.info("Processing message with ID: {}", messageDto.getId());
    }
}
