package com.course.kafka.broker.stream.web;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebDesingVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.util.WebColorVoteTimestampExtractor;
import com.course.kafka.util.WebLayoutVoteTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class WebDesignVoteThreeStream {

    @Bean
    public KStream<String, WebDesingVoteMessage> kstreamWebDesingVote(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var colorSerde = new JsonSerde<>(WebColorVoteMessage.class);
        var layoutSerde = new JsonSerde<>(WebLayoutVoteMessage.class);
        var desingSerde = new JsonSerde<>(WebDesingVoteMessage.class);

        // color
        builder.stream("t.commodity.web.vote-color",
                Consumed.with(stringSerde, colorSerde, new WebColorVoteTimestampExtractor(), null))
                .mapValues(v -> v.getColor()).to("t.commodity.web.vote-three-stream-color");
        var colorTable = builder.table("t.commodity.web.vote-three-username-color",
                Consumed.with(stringSerde, stringSerde));

        // layout
        builder.stream("t.commodity.web.vote-layout",
                Consumed.with(stringSerde, layoutSerde, new WebLayoutVoteTimestampExtractor(), null))
                .mapValues(v -> v.getLayout()).to("t.commodity.web.vote-three-stream-layout");
        var layaoutTable = builder.table("t.commodity.web.vote-three-username-layout",
                Consumed.with(stringSerde, stringSerde));

        // join
        var joinTable = colorTable.outerJoin(layaoutTable, this::voteJoiner, Materialized.with(stringSerde, desingSerde));
        joinTable.toStream().to("t.commodity.web.vote-three-result");

        // vote result
        joinTable.groupBy((username, voteDesign) -> KeyValue.pair(voteDesign.getColor(), voteDesign.getColor()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote three - color"));
        joinTable.groupBy((username, voteDesign) -> KeyValue.pair(voteDesign.getLayout(), voteDesign.getLayout()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote three - layaout"));

        return joinTable.toStream();
    }

    private WebDesingVoteMessage voteJoiner(String color, String layout) {
        var result = new WebDesingVoteMessage();

        result.setColor(color);
        result.setLayout(layout);

        return  result;
    }

}
