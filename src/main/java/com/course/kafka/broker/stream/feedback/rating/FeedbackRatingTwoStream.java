package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingOneMessage;
import com.course.kafka.broker.message.FeedbackRatingTwoMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class FeedbackRatingTwoStream {


    public KStream<String, FeedbackMessage> kstreamFeedbackRating(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);
        var feedbackRatingTwoSerde = new JsonSerde<>(FeedbackRatingTwoMessage.class);
        var feedbackRatingTwoStoreValueSerde = new JsonSerde<>(FeedbackRatingTwoStoreValue.class);

        var feedbackStream =builder.stream("t.commodity.feedback", Consumed.with(stringSerde, feedbackSerde));

        var feedbackRatingStateStoreName = "feedbackRatingTwoStateStore";
        var storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);
        var storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, feedbackRatingTwoStoreValueSerde);

        builder.addStateStore(storeBuilder);

        feedbackStream.transformValues(()-> new FeedbackRatingTwoTransformer(feedbackRatingStateStoreName), feedbackRatingStateStoreName)
                .to("t.commodity.feedback.rating-two", Produced.with(stringSerde, feedbackRatingTwoSerde));

        return feedbackStream;
    }

}
