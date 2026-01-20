package com.work.producers;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.work.dto.MessageDto;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class AvroProducer {
    private static final Logger logger = LoggerFactory.getLogger(AvroProducer.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "127.0.0.1:9094");
        String schemaRegistry = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://127.0.0.1:8081");
        String topic = System.getenv().getOrDefault("TOPIC", "test-topic");

        int sendingInitialDelayInMillis = 0; //Задержка запуска отправки сообщений в мс.
        int sendingPeriodInMillis = 500; // Периодичность отправки сообщений в мс.

        logger.info("Starting producer on {}...", bootstrapServers);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Список адресов брокеров кластера Kafka.
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Сериализатор ключа сообщения String -> byte[].
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName()); // Сериализатор значения сообщения object -> byte[] (Avro).
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry); // Адрес реестра для регистрации и проверки Avro-схем.
        properties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true); // Включает поддержку логических типов Avro (в проекте используется для работы с UUID как с объектом Java).
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, true); // Включает генерацию Avro-схемы через Reflection (не требует .avsc-файла для MessageDto).
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100); // Пауза в мс между попытками переотправки сообщения при ошибке.
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Предотвращает дублирование сообщений при сетевых сбоях и сохраняет их очередность (автоматически настраивает acks=all и retries=MAX_VALUE).
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Количество попыток переотправки при временных сбоях (MAX_VALUE гарантирует упорство продюсера - бесконечные попытки).
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); // Уровень подтверждения записи: все реплики должны подтвердить получение сообщения.
        
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties);
        Random random = new Random();

        Runnable sendingTask = () -> {
            try {
                MessageDto messageDto = new MessageDto(UUID.randomUUID(), random.nextInt(10));

                // Отправка записи в топик (ключ: UUID сообщения, значение: MessageDto).
                kafkaProducer.send(new ProducerRecord<String, Object>(topic, messageDto.getId().toString(), messageDto), (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent [Bytes: {}] [ID: {}] [Number: {}] [Topic: {}] [Partition: {}] [Offset: {}].", 
                             metadata.serializedValueSize(), messageDto.getId(), messageDto.getNumber(), metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send message with ID: {}.", messageDto.getId(), exception);
                    }
                });
            } catch (Exception exception) {
                logger.error("Unexpected scheduler error.", exception);
            }
        };

        // Запуск задачи отправки с фиксированным интервалом (500 мс).
        scheduler.scheduleAtFixedRate(sendingTask, sendingInitialDelayInMillis, sendingPeriodInMillis , TimeUnit.MILLISECONDS);

        // Обработка корректного завершения приложения (graceful shutdown).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(sendingPeriodInMillis, TimeUnit.MILLISECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException exception) {
                scheduler.shutdownNow();
            }
            kafkaProducer.close();
            logger.info("Producer closed.");
        }));
    }
}
