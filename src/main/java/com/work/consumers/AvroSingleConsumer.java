package com.work.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;

import com.work.dto.MessageDto;

public class AvroSingleConsumer extends AvroConsumer {
    public static void main(String[] args) {
        String groupId = System.getenv().getOrDefault("GROUP_ID", "single-group");
        String minimumFetchInBytes = System.getenv().getOrDefault("FETCH_MIN_BYTES", "1");        
        int maximumFetchWaitInMillis = Integer.parseInt(System.getenv().getOrDefault("FETCH_MAX_WAIT_MS", "100"));

        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(getKafkaProperties(groupId, true, minimumFetchInBytes, maximumFetchWaitInMillis));

        setupShutdownHook(kafkaConsumer, Thread.currentThread());

        try {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

            logger.info("Single consumer started and subscribed to {}.", TOPIC);

            while (true) {
                // Запрос новой порции данных, при отсутствии данных блокирует поток на 100 мс. 
                ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (consumerRecords.isEmpty()){
                    continue;
                }

                // Получение метаданных о партициях для определения брокера сообщения.
                List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(TOPIC);

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
}
