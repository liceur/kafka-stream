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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import static com.course.kafka.util.CommodityStreamUtil.*;
import static org.apache.kafka.streams.state.RocksDBConfigSetter.LOG;

//@Configuration
public class CommodityFiveStream {

    private static final Logger LOG = LoggerFactory.getLogger(CommodityFiveStream.class);

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
        final var branchProducer = Produced.with(stringSerde, orderPatterSerde);

        new KafkaStreamBrancher<String, OrderPatterMessage>()
               .branch(isPlastic(), kstream -> kstream.to("t.commodity.pattern-five.plastic", branchProducer))
               .defaultBranch(kstream -> kstream.to("t.commodity.pattern-five.noPlastic", branchProducer))
               .onTopOf(maskedOrderStream.mapValues(CommodityStreamUtil::mapToOrderPattern));

        // 2sn sink stream to reward
        KStream<String, OrderRewardMessage> rewardStream = maskedOrderStream
                .filterNot(isCheap())
                .map(CommodityStreamUtil.mapToOrderRewardChangeKey());
        rewardStream.to("t.commdity.reward-five", Produced.with(stringSerde, orderRewardSerde));

        // 3rd sink stream to storage
        KStream<String, OrderMessage> storageStream = maskedOrderStream
                .selectKey(generateStorageKey());
        storageStream.to("t.commodity.storage-five", Produced.with(stringSerde, orderSerde));

        // 4st stream for fraud
        maskedOrderStream.filter((k, v) -> v.getOrderLocation().toUpperCase().startsWith("C")).foreach((k,v) -> this.reportFraud(v));

        return maskedOrderStream;
    }

    private void reportFraud(OrderMessage v) {
        LOG.info("Reporting fraud {}", v);
    }

}
