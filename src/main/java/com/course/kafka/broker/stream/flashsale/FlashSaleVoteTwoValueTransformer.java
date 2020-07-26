package com.course.kafka.broker.stream.flashsale;

import com.course.kafka.broker.message.FlashSaleVoteMessage;
import com.course.kafka.util.LocalDateTimeUtil;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.LocalDateTime;

public class FlashSaleVoteTwoValueTransformer implements ValueTransformer<FlashSaleVoteMessage, FlashSaleVoteMessage> {

    private final long voteStartTime;
    private final long voteEndTime;
    private ProcessorContext processorContext;

    public FlashSaleVoteTwoValueTransformer(LocalDateTime voteStart, LocalDateTime voteEnd) {
        this.voteStartTime = LocalDateTimeUtil.toEpochTimestamp(voteStart);
        this.voteEndTime = LocalDateTimeUtil.toEpochTimestamp(voteEnd);
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public FlashSaleVoteMessage transform(FlashSaleVoteMessage flashSaleVoteMessage) {
        var recordTime = processorContext.timestamp();

        return (recordTime >= voteStartTime && recordTime <= voteEndTime)? flashSaleVoteMessage: null;
    }

    @Override
    public void close() {

    }
}
