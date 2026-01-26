package com.work.consumers;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.work.components.MessagingComponent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public abstract class AvroConsumer extends MessagingComponent {
    // Разница между максимальным временем накопления данных брокером в топике и тайм-аутом сетевого запроса.
    protected static final int REQUEST_TIMEOUT_FETCH_MAX_WAIT_DELTA_MS = Integer.parseInt(System.getenv().getOrDefault("REQUEST_TIMEOUT_FETCH_MAX_WAIT_DELTA_MS", "2000"));
    // Время ожидания закрытия потребителя при выходе из приложения.
    protected static final int GRACEFUL_SHUTDOWN_WAIT_MS = Integer.parseInt(System.getenv().getOrDefault("GRACEFUL_SHUTDOWN_WAIT_MS", "5000"));

    protected static Properties getKafkaProperties(String groupId, boolean enableAutoCommit, String minimumFetchInBytes, int maximumFetchWaitInMillis){
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // Список адресов брокеров кластера Kafka.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Уникальный идентификатор группы потребителей (позволяет балансировать партиции между репликами приложения).
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Сериализатор ключа сообщения String -> byte[].
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName()); // Сериализатор значения сообщения object -> byte[] (Avro).
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY); // Адрес реестра для регистрации и проверки Avro-схем.
        properties.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true); // Включает поддержку логических типов Avro (в проекте используется для работы с UUID как с объектом Java).
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, true); // Включает генерацию Avro-схемы через Reflection (не требует .avsc-файла для MessageDto).
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit); // Включает/отключает автоматическое подтверждение прочитанных смещений (offsets).
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, SENDING_PERIOD_MS); // Периодичность автоматических подтверждений прочитанных смещений в мс.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Стратегия поведения при отсутствии смещения: "earliest" заставляет читать топик с самого начала.
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minimumFetchInBytes); // Минимальное количество данных в топике в байтах, которое брокер должен накопить перед ответом потребителю.
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maximumFetchWaitInMillis); // Максимальное время накопления данных брокером в топике перед ответом потребителю.
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_FETCH_MAX_WAIT_DELTA_MS + maximumFetchWaitInMillis); // Тайм-аут сетевого запроса к брокеру, должен быть больше FETCH_MAX_WAIT_MS_CONFIG + время обработки poll().
        
        return properties;
    }

    // Обработка корректного завершения приложения (graceful shutdown): при выключении контейнера у потребителя вызовется wakeup() для выхода из цикла poll().
    protected static void setupShutdownHook(KafkaConsumer<String, Object> kafkaConsumer, Thread mainThread){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Waiting for consumer to shut down...");
            kafkaConsumer.wakeup();
            try {
                mainThread.join(GRACEFUL_SHUTDOWN_WAIT_MS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
