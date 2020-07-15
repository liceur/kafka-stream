package com.course.kafka.broker.stream.promotion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class PromotionUppercaseStream {

    @Bean
    public KStream<String, String> kstreamPromotionUppercase(StreamsBuilder builder){
        KStream<String, String> sourceStream = builder.stream("t.commodity.promotion",
                                                                    Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> uppercaStream = sourceStream.mapValues(s -> s.toUpperCase());
        uppercaStream.to("t.commodity.promotion-uppercase");

        // Don't use this on production
        sourceStream.print(Printed.<String, String>toSysOut().withLabel("Original Stream"));
        uppercaStream.print(Printed.<String, String>toSysOut().withLabel("Uppercase Stream"));

        return sourceStream;
    }


}
