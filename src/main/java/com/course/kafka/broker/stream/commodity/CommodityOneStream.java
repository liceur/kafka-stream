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
import org.springframework.core.annotation.Order;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class CommodityOneStream {

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
        KStream<String, OrderPatterMessage> patternStream = maskedOrderStream
                .mapValues(CommodityStreamUtil::mapToOrderPattern);

        patternStream.to("t.commodity.pattern-one", Produced.with(stringSerde, orderPatterSerde));

        // 2sn sink stream to reward
        // filter only "large" quantity
        KStream<String, OrderRewardMessage> rewardStream = maskedOrderStream
                .filter(CommodityStreamUtil.isLargeQuantity())
                .mapValues(CommodityStreamUtil::mapToRewarMessage);
        rewardStream.to("t.commdity.reward-one", Produced.with(stringSerde, orderRewardSerde));

        // 3rd sink stream to storage
        // no transformation
        maskedOrderStream.to("t.commodity.storage-one", Produced.with(stringSerde, orderSerde));

        return maskedOrderStream;
    }

}
