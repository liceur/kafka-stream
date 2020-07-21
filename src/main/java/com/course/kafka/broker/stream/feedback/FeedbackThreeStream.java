package com.course.kafka.broker.stream.feedback;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Configuration
public class FeedbackThreeStream {

    private static final Set<String> BAD_WORDS = Set.of("angry", "bad", "sad");
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpfull");

    @Bean
    public KStream<String, FeedbackMessage> kstreamFeedback(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        var sourceStream = builder.stream("t.commodity.feedback", Consumed.with(stringSerde, feedbackSerde));

        var feedbackStream = sourceStream.flatMap(splitWords()).branch(isGoodWord(), isBadWord());

        feedbackStream[0].to("t.commodity.feedback-three-good");
        feedbackStream[1].to("t.commodity.feedback-three-bad");

        return sourceStream;
    }

    private Predicate<? super String,? super String> isBadWord() {
        return (k,v)-> BAD_WORDS.contains(v);
    }

    private Predicate<? super String,? super String> isGoodWord() {
        return (k,v)-> GOOD_WORDS.contains(v);
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> splitWords() {
        return (key, value) -> Arrays
                .asList(value.getFeedback().replaceAll("[^a-zA-Z]", "").toLowerCase().split("\\s+"))
                .stream().distinct().map(word -> KeyValue.pair(value.getLocation(), word)).collect(toList());
    }

    private ValueMapper<FeedbackMessage, Iterable<String>> mapperGoodWords() {
        return feeedbackMessage -> Arrays.asList(feeedbackMessage.getFeedback().replaceAll("[^a-zA-Z]", "")
                .toLowerCase().split("\\s+"))
                .stream().filter(word -> GOOD_WORDS.contains(word)).distinct().collect(toList());
    }

}
