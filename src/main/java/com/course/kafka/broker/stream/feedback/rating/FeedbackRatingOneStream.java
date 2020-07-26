package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingOneMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class FeedbackRatingOneStream {


    public KStream<String, FeedbackMessage> kstreamFeedbackRating(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);
        var feedbackRatingOneSerde = new JsonSerde<>(FeedbackRatingOneMessage.class);
        var feedbackRatingOneStoreValueSerde = new JsonSerde<>(FeedbackRatingOneStoreValue.class);

        var feedbackStream =builder.stream("t.commodity.feedback", Consumed.with(stringSerde, feedbackSerde));

        var feedbackRatingStateStoreName = "feedbackRatingOneStateStore";
        var storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);
        var storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, feedbackRatingOneStoreValueSerde);

        builder.addStateStore(storeBuilder);

        feedbackStream.transformValues(()-> new FeedbackRatingOneTransformer(feedbackRatingStateStoreName), feedbackRatingStateStoreName)
                .to("t.commodity.feedback.rating-one", Produced.with(stringSerde, feedbackRatingOneSerde));

        return feedbackStream;
    }

}
