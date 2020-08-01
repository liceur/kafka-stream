package com.course.kafka.util;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WebLayoutVoteTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        var message = (WebLayoutVoteMessage)consumerRecord.value();

        return message != null
                ? LocalDateTimeUtil.toEpochTimestamp(message.getVoteDateTime())
                : consumerRecord.timestamp();
    }
}
