    package com.work.consumers;

    import java.time.Duration;
    import java.util.Collections;
    import java.util.LinkedList;
    import java.util.List;
    import java.util.Properties;
    import java.util.Queue;

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

    import com.work.dto.MessageDto;

    import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
    import io.confluent.kafka.serializers.KafkaAvroDeserializer;
    import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

    public class AvroBatchConsumer {
        private static final Logger logger = LoggerFactory.getLogger(AvroBatchConsumer.class);

        public static void main(String[] args) {
            String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "127.0.0.1:9094");
            String schemaRegistry = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://127.0.0.1:8081");
            String groupId = System.getenv().getOrDefault("GROUP_ID", "batch-group");
            String topic = System.getenv().getOrDefault("TOPIC", "test-topic");

            int batchSizeInMessages = 10; // Размер батча сообщений.
            int sendingPeriodInMillis = 500; // Периодичность отправки сообщений продюсером в мс.
            int waitingTimeInMillis = batchSizeInMessages * sendingPeriodInMillis; // Время ожидания накопления одного батча (5000 мс).
            int timeoutInMillis = waitingTimeInMillis + 2000; // Тайм-аут сетевого запроса к брокеру (7000 мс).

            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Список адресов брокеров кластера Kafka.
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Уникальный идентификатор группы потребителей (позволяет балансировать партиции между репликами приложения).
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Сериализатор ключа сообщения String -> byte[].
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName()); // Сериализатор значения сообщения object -> byte[] (Avro).
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry); // Адрес реестра для регистрации и проверки Avro-схем.
            properties.put(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true); // Включает поддержку логических типов Avro (в проекте используется для работы с UUID как с объектом Java).
            properties.put(KafkaAvroDeserializerConfig.SCHEMA_REFLECTION_CONFIG, true); // Включает генерацию Avro-схемы через Reflection (не требует .avsc-файла для MessageDto).
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Отключает автоматическое подтверждение прочитанных смещений (offsets) - будет подтверждаться вручную после обработки батча.
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Стратегия поведения при отсутствии смещения: "earliest" заставляет читать топик с самого начала.    
            properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 5000); // Минимальное количество данных в топике в байтах, которое брокер должен накопить перед ответом потребителю. Устанавливается заведомо
            // большее значение, т.к. отталкиваемся не от объема всех сообщений (он размазан по партициям в разных брокерах), а от времени, за которое продюсер отправит достаточное число сообщений для одного батча.
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSizeInMessages); // Максимальное количество записей, возвращаемых брокером за один вызов poll() - установлено равным размеру батча.
            properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, waitingTimeInMillis); // Максимальное время накопления данных брокером в топике перед ответом потребителю.
            properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutInMillis); // Тайм-аут сетевого запроса к брокеру, должен быть больше FETCH_MAX_WAIT_MS_CONFIG + время обработки poll().

            KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties);

             // Внутренний буфер для накопления записей между вызовами poll().
            Queue<ConsumerRecord<String, Object>> consumerRecordsBuffer = new LinkedList<>();

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

            // Время обработки последнего poll() в мс.
            long lastProcessedTimeInMillis = System.currentTimeMillis();

            try {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
                logger.info("Batch consumer started and subscribed to {}. Goal: {} messages per batch.", topic, batchSizeInMessages);

                while (true) {
                    /**Запрос новой порции данных для размещения в буффере, при отсутствии данных блокирует поток на 1 секунду.
                     * По истечении FETCH_MAX_WAIT_MS_CONFIG от брокеров приходит несколько параллельных ответов, каждый из них
                     * ограничен 10 записями благодаря MAX_POLL_RECORDS_CONFIG. Сумма всех ответов составляет >=10 сообщений.*/
                    ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                    
                    if (!consumerRecords.isEmpty()){
                        for (ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
                            consumerRecordsBuffer.add(consumerRecord);
                        }
                        logger.info("Buffering {} messages ({}/{}).", consumerRecords.count(), consumerRecordsBuffer.size(), batchSizeInMessages);
                    }

                    // Условия для начала обработки батча: собрано достаточное количество сообщений в буффере (>=10) или вышло время ожидания запроса.
                    boolean isFullBatch = consumerRecordsBuffer.size() >= batchSizeInMessages;
                    boolean isTimeOut = System.currentTimeMillis() - lastProcessedTimeInMillis > timeoutInMillis;

                    if (!isFullBatch && (!isTimeOut || consumerRecordsBuffer.isEmpty())) {
                        // Фиксирование времени обработки последнего poll() и запрос новых данных при пустом буффере.
                        if (consumerRecordsBuffer.isEmpty()) {
                            lastProcessedTimeInMillis = System.currentTimeMillis();
                        }
                        continue;
                    }

                    // Установка размера батча (<= 10 сообщений).
                    int currentBatchSizeInMessages = Math.min(consumerRecordsBuffer.size(), batchSizeInMessages);
                    logger.info("Start receiving batch of {} messages...", currentBatchSizeInMessages);
                              
                    // Получение метаданных о партициях для определения брокера сообщения.
                    List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);

                    for (int index = 0; index < currentBatchSizeInMessages; index++) {
                        ConsumerRecord<String, Object> consumerRecord = consumerRecordsBuffer.poll();

                        try{
                            MessageDto messageDto = (MessageDto) consumerRecord.value();

                            // Определение брокера сообщения.
                            int brokerId = partitions
                                .stream()
                                .filter(streamPartition -> streamPartition.partition() == consumerRecord.partition())
                                .map(streamPartition -> streamPartition.leader().id())
                                .findFirst()
                                .orElse(-1);
                        
                            logger.info("Received #{} [Bytes: {}] [BrokerId: {}] [ID: {}] [Number: {}] [Topic: {}] [Partition: {}] [Offset: {}].", 
                                index + 1, consumerRecord.serializedValueSize(), brokerId, messageDto.getId(), messageDto.getNumber(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                            
                            // Некоторая бизнес-логика обработки сообщения.
                            processMessage(messageDto);

                        } catch (ClassCastException | SerializationException exception) {
                            logger.error("Failed to deserialize message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);
                        
                        } catch (Exception exception) {
                            logger.error("Failed to process message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);
                        }
                    }
                    
                    // Ручное подтверждение прочитанных смещений после обработки батча (гарантирует доставку "at least once"). Фиксирование времени обработки последнего poll().
                    kafkaConsumer.commitSync();
                    lastProcessedTimeInMillis = System.currentTimeMillis();
                    logger.info("Batch offsets committed. Buffer: {}/{}.", consumerRecordsBuffer.size(), batchSizeInMessages);
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
