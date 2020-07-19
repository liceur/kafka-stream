package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatterMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import static com.course.kafka.util.CommodityStreamUtil.*;
import static com.course.kafka.util.CommodityStreamUtil.isCheap;
import static com.course.kafka.util.CommodityStreamUtil.isPlastic;

//@Configuration
public class CommodityTwoStream {

    @Bean
    public KStream<String, OrderMessage> kstreamCommodityTrading(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OrderMessage.class);
        var orderPatterSerde = new JsonSerde<>(OrderPatterMessage.class);
        var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedOrderStream = builder
                .stream("t.commodity.order", Consumed.with(stringSerde, orderSerde))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        // 1st sink strem to pattern
        // summarize order item ( total = price * quantity
        KStream<String, OrderPatterMessage>[] patternStream = maskedOrderStream
                .mapValues(CommodityStreamUtil::mapToOrderPattern)
                .branch(isPlastic(), (k, v) -> true);

        int plasticIndex = 0;
        int notPlasticIndex = 1;

        patternStream[plasticIndex].to("t.commodity.pattern-two.plastic",
                Produced.with(stringSerde, orderPatterSerde));
        patternStream[notPlasticIndex].to("t.commodity.pattern-two.notplastic",
                Produced.with(stringSerde, orderPatterSerde));

        // 2sn sink stream to reward
        KStream<String, OrderRewardMessage> rewardStream = maskedOrderStream
                .filterNot(isCheap())
                .mapValues(CommodityStreamUtil::mapToRewarMessage);
        rewardStream.to("t.commdity.reward-two", Produced.with(stringSerde, orderRewardSerde));

        // 3rd sink stream to storage
        KStream<String, OrderMessage> storageStream = maskedOrderStream
                .selectKey(generateStorageKey());
        storageStream.to("t.commodity.storage-two", Produced.with(stringSerde, orderSerde));

        return maskedOrderStream;
    }

}
