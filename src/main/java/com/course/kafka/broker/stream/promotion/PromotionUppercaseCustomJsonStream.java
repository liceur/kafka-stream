package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.course.kafka.broker.serde.PromotionSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class PromotionUppercaseCustomJsonStream {

    @Bean
    public KStream<String, PromotionMessage> kStreamPromotionUpperCase(StreamsBuilder builder){

        var stringSerde = Serdes.String();
        var jsonSerde = new PromotionSerde();

        KStream<String, PromotionMessage> sourceStream = builder.stream("t.commidity.promotion",
                                                                        Consumed.with(stringSerde, jsonSerde));
        KStream<String, PromotionMessage> uppercaseStream = sourceStream.mapValues(this::upperCasePromotionCode );

        uppercaseStream.to("t.commodity.promotion-uppercase", Produced.with(stringSerde, jsonSerde));


        // Not use in Production
        sourceStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("JSON Original Stream"));
        uppercaseStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("JSON Uppercase Stream"));

        return sourceStream;
    }

    private PromotionMessage upperCasePromotionCode (PromotionMessage message){
        return new PromotionMessage(message.getPromotionalCode().toUpperCase());
    }


}
