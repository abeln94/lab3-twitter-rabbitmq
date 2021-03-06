package es.unizar.tmdad.lab3.solution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.social.twitter.api.HashTagEntity;
import org.springframework.social.twitter.api.Tweet;

@Configuration
public class TwitterFlowAggregator {

    final static String AGGREGATOR_FANOUT_EXCHANGE = "twitter_fanout";
    final static String AGGREGATOR_FANOUT_A_QUEUE_NAME = "aggregator_fanout_queue";

    @Autowired
    RabbitTemplate rabbitTemplate;

    // Configuración RabbitMQ
    @Bean
    Queue aggregatorFanoutQueue() {
        return new Queue(AGGREGATOR_FANOUT_A_QUEUE_NAME, false);
    }

    @Bean
    FanoutExchange aggregatorFanoutExchange() {
        return new FanoutExchange(AGGREGATOR_FANOUT_EXCHANGE);
    }

    @Bean
    Binding aggregatorFanoutBinding() {
        return BindingBuilder.bind(aggregatorFanoutQueue()).to(aggregatorFanoutExchange());
    }

    // Flujo #3
    //
    // MessageEndpoint RabbitMQ -(requestChannelRabbitMQ)-> tareas ...
    //
    @Bean
    public DirectChannel requestChannelAggregator() {
        return MessageChannels.direct().get();
    }

    @Bean
    public AmqpInboundChannelAdapter aggregatorAmqpInbound() {
        SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer(
                rabbitTemplate.getConnectionFactory());
        smlc.setQueues(aggregatorFanoutQueue());
        return Amqp.inboundAdapter(smlc)
                .outputChannel(requestChannelAggregator()).get();
    }

    // -- //
    @Bean
    public IntegrationFlow sendtrendingTopics() {
        return IntegrationFlows
                .from(requestChannelAggregator())
                .log("aggregator")
                .filter("payload instanceof T(org.springframework.social.twitter.api.Tweet)")
                .aggregate(aggregationSpec())
                .transform(getTrendingTopics())
                .handle("streamSendingService", "sendTrends").get();
    }

    private Consumer<AggregatorSpec> aggregationSpec() {
        return a -> a.correlationStrategy(m -> 1)
                .releaseStrategy(g -> g.size() == 1000)
                .expireGroupsUponCompletion(true);
    }

    private GenericTransformer<List<Tweet>, List<Map.Entry<String, Integer>>> getTrendingTopics() {
        return tlist -> {
            Map<String, Integer> hashCodes = tlist.stream()
                    .flatMap(t -> t.getEntities().getHashTags().stream())
                    .collect(Collectors.groupingBy(HashTagEntity::getText, Collectors.reducing(0, s -> 1, Integer::sum)));

            List<Entry<String, Integer>> list = new ArrayList<>(hashCodes.entrySet());
            list.sort(Collections.reverseOrder(Entry.comparingByValue()));

            return list.subList(0, 10);
        };
    }
}
