package es.unizar.tmdad.lab3.flows;

import es.unizar.tmdad.lab3.domain.MyTweet;
import es.unizar.tmdad.lab3.domain.TargetedTweet;
import es.unizar.tmdad.lab3.service.TwitterLookupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.support.Function;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.social.twitter.api.Tweet;

import java.util.stream.Collectors;

abstract public class TwitterFlowCommon {

    @Autowired
    private TwitterLookupService tls;

    @Bean
    public IntegrationFlow sendTweet() {
        return IntegrationFlows
                .from(requestChannelRabbitMQ())
                .filter("payload instanceof T(org.springframework.social.twitter.api.Tweet)")
                .transform(identifyTopics())
                .split(TargetedTweet.class, duplicateByTopic())
                .transform(highlight())
                .handle("streamSendingService", "sendTweet").get();
    }

    abstract protected AbstractMessageChannel requestChannelRabbitMQ();

    private GenericTransformer<TargetedTweet, TargetedTweet> highlight() {
        return t -> {
            String tag = t.getFirstTarget();
            String text = t.getTweet().getUnmodifiedText();
            t.getTweet().setUnmodifiedText(
                    text.replaceAll(tag, "<span class=\"badge badge-pill badge-danger\">" + tag + "</span>"));
            return t;
        };
    }

    private Function<TargetedTweet, ?> duplicateByTopic() {
        return t -> t.getTargets().stream()
                .map(x -> new TargetedTweet(t.getTweet(), x))
                .collect(Collectors.toList());
    }

    private GenericTransformer<Tweet, TargetedTweet> identifyTopics() {
        return t -> new TargetedTweet(new MyTweet(t), tls.getQueries().stream()
                .filter(x -> t.getText().contains(x))
                .collect(Collectors.toList()));
    }

}
