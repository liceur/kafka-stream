package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


//@Configuration
public class PromotionUppercaseJsonStream {

        private static final Logger LOG = LoggerFactory.getLogger(PromotionUppercaseJsonStream.class);

        private ObjectMapper objectMapper = new ObjectMapper();

        @Bean
        public KStream<String, String> kStreamPromotionUpperCase(StreamsBuilder streamsBuilder){
            var stringSerde = Serdes.String();

            KStream<String, String> sourceStream = streamsBuilder.stream("t.commodity.promotion",
                    Consumed.with(stringSerde, stringSerde));

            KStream<String, String> uppercaseStream = sourceStream.mapValues(this::uppercasePromotionCode);

            uppercaseStream.to("t.commodity.promotion-uppercase");

            // Not use in Production
            sourceStream.print(Printed.<String, String>toSysOut().withLabel("JSON Original Stream"));
            uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("JSON Uppercase Stream"));

            return sourceStream;
        }

        private String uppercasePromotionCode(String message){
            try {
                var original = objectMapper.readValue(message, PromotionMessage.class);

                var converted = new PromotionMessage(original.getPromotionalCode().toUpperCase());
                return objectMapper.writeValueAsString(converted);
            } catch (JsonProcessingException e) {
                LOG.warn("Can't process message {}", message);
            }

            return StringUtils.EMPTY;
        }

}
