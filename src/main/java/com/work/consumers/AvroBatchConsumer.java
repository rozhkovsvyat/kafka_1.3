package com.work.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

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

        setupShutdownHook(kafkaConsumer, Thread.currentThread());
        
        try {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
            logger.info("Batch consumer started and subscribed to {}. Goal: {} messages per batch.", TOPIC, batchSizeInMessages);

            while (true) {
                /**Запрос новой порции данных, при отсутствии данных блокирует поток на 1 секунду.
                 * По истечении FETCH_MAX_WAIT_MS_CONFIG от брокеров приходит несколько параллельных ответов.
                 * Сумма сообщений в ответах ограничена 10 записями благодаря MAX_POLL_RECORDS_CONFIG.*/
                ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                
                if (consumerRecords.isEmpty()){
                    continue;
                }

                logger.info("Received batch of {} messages.", consumerRecords.count());
                          
                int index = 0;
                // Получение метаданных о партициях для определения брокера сообщения.
                List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(TOPIC);

                for (ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
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
                            ++index, consumerRecord.serializedValueSize(), brokerId, messageDto.getId(), messageDto.getNumber(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                        
                        processMessage(messageDto);

                    } catch (ClassCastException | SerializationException exception) {
                        logger.error("Failed to deserialize message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);
                    
                    } catch (Exception exception) {
                        logger.error("Failed to process message at partition {} offset {}.", consumerRecord.partition(), consumerRecord.offset(), exception);
                    }
                }
                
                // Ручное подтверждение прочитанных смещений после обработки батча (гарантирует доставку "at least once"). Фиксирование времени обработки последнего poll().
                kafkaConsumer.commitSync();
                logger.info("Batch offsets committed.");
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
}
