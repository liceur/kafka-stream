package com.course.kafka.util;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;

public class InventoryTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
        var inventoryMessage = (InventoryMessage)consumerRecord.value();

        return inventoryMessage != null ? LocalDateTimeUtil.toEpochTimestamp(inventoryMessage.getTransactiontimel())
                : consumerRecord.timestamp();
    }
}
