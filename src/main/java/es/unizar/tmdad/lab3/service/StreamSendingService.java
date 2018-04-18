package es.unizar.tmdad.lab3.service;

import es.unizar.tmdad.lab3.domain.TargetedTweet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.social.twitter.api.FilterStreamParameters;
import org.springframework.social.twitter.api.Stream;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.impl.TwitterTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class StreamSendingService {

    @Autowired
    private StreamListener integrationStreamListener;

    @Autowired
    private SimpMessageSendingOperations ops;

    @Autowired
    private TwitterTemplate twitterTemplate;

    private Stream stream;

    @PostConstruct
    public void initialize() {
        FilterStreamParameters fsp = new FilterStreamParameters();
        fsp.addLocation(-180, -90, 180, 90);
        stream = twitterTemplate.streamingOperations().filter(fsp,
                Arrays.asList(integrationStreamListener));
    }

    public Stream getStream() {
        return stream;
    }

    public void sendTweet(TargetedTweet targeted) {
        Map<String, Object> map = new HashMap<>();
        map.put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);
        ops.convertAndSend("/queue/search/" + targeted.getFirstTarget(),
                targeted.getTweet(), map);
    }

    public void sendTrends(List<Map.Entry<String, Integer>> targeted) {
        Map<String, Object> map = new HashMap<>();
        map.put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);
        ops.convertAndSend("/queue/trends",
                targeted, map);
    }
}
