package es.unizar.tmdad.lab3.solution;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQEndpoint {
    
    //exchanges
    static final String tweetsExchangeName = "twitter_fanout";
    
    @Bean
    FanoutExchange tweetsExchange() {
        return new FanoutExchange(tweetsExchangeName);
    }
    
    //queues
    static final String inputQueueName = "aggregator_fanout_queue";
    
    @Bean
    Queue saveTweetsQueue(){
        return new Queue(inputQueueName, false);
    }
    
    
    //bindings
    @Bean
    Binding saveTweetsBinding() {
        return BindingBuilder.bind(saveTweetsQueue()).to(tweetsExchange());
    }

    //redirections
    @Bean
    SimpleMessageListenerContainer saveTweetsContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(saveTweetsQueue());
        container.setMessageListener(saveTweetsAdapter());
        return container;
    }

    
    //adapters
    @Bean
    MessageListenerAdapter saveTweetsAdapter() {
        return new MessageListenerAdapter(this, "receiveMessage");
    }
    
    @Autowired
    TwitterFlowAggregator aggregator;
    
    //listeners
    public void receiveMessage(Tweet tweet) {
        aggregator.requestChannelTwitter().send(new MessageTweet(tweet));
    }

    private static class MessageTweet implements Message<Tweet> {
        Tweet tweet;

        private MessageTweet(Tweet tweet) {
            this.tweet = tweet;
        }

        @Override
        public Tweet getPayload() {
            return tweet;
        }

        @Override
        public MessageHeaders getHeaders() {
            return new MessageHeaders(null);
        }
    }
    
}
