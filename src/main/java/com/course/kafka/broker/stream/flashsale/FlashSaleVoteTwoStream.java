package com.course.kafka.broker.stream.flashsale;


import com.course.kafka.broker.message.FlashSaleVoteMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Configuration
public class FlashSaleVoteTwoStream {

    @Bean
    public KStream<String, String> kstreamFlashSaleVote(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var flashSaleVoteSerde = new JsonSerde<>(FlashSaleVoteMessage.class);

        var voteStart = LocalDateTime.of(LocalDate.now(), LocalTime.of(9,0));
        var voteEnd = LocalDateTime.of(LocalDate.now(), LocalTime.of(10,0));

        var flashSaleVoteSream = builder
                .stream("t.commodity.flashsale.vote", Consumed.with(stringSerde, flashSaleVoteSerde))
                .transformValues(() -> new FlashSaleVoteTwoValueTransformer(voteStart, voteEnd))
                .filter((key, transformedValue) -> transformedValue != null)
                .map((key,value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()));
        flashSaleVoteSream.to("t.commodity.flashsale.vote-user-item");

        // table
        builder.table("t.commodity.flashsale.vote-user-item", Consumed.with(stringSerde, stringSerde))
                .groupBy((user, votedItem) -> KeyValue.pair(votedItem, votedItem)).count().toStream()
                .to("t.commodity.flash.vote-two-result", Produced.with(stringSerde, Serdes.Long()));

        return flashSaleVoteSream;
    }

}
