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

import com.work.dto.MessageDto;

public class AvroBatchConsumer extends AvroConsumer {
    public static void main(String[] args) {
        String groupId = System.getenv().getOrDefault("GROUP_ID", "batch-group");
        String minimumFetchInBytes = System.getenv().getOrDefault("FETCH_MIN_BYTES", "5000");   
        int batchSizeInMessages = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "10"));         
        int maximumFetchWaitInMillis = batchSizeInMessages * SENDING_PERIOD_MS;

        Properties properties = getKafkaProperties(groupId, false, minimumFetchInBytes, maximumFetchWaitInMillis);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSizeInMessages); // Максимальное число записей, возвращаемых брокером за один вызов poll() (установлено равным размеру батча).
        
        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties);

         // Внутренний буфер для накопления записей между вызовами poll().
        Queue<ConsumerRecord<String, Object>> consumerRecordsBuffer = new LinkedList<>();

        setupShutdownHook(kafkaConsumer, Thread.currentThread());
        
        // Время обработки последнего poll() в мс.
        long lastProcessedTimeInMillis = System.currentTimeMillis();
        try {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
            logger.info("Batch consumer started and subscribed to {}. Goal: {} messages per batch.", TOPIC, batchSizeInMessages);
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
                boolean isTimeOut = System.currentTimeMillis() - lastProcessedTimeInMillis > REQUEST_TIMEOUT_FETCH_MAX_WAIT_DELTA_MS + maximumFetchWaitInMillis;
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
                List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(TOPIC);
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
