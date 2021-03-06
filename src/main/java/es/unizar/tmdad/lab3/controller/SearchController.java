package es.unizar.tmdad.lab3.controller;

import es.unizar.tmdad.lab3.service.TwitterLookupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class SearchController {

    @Autowired
    TwitterLookupService twitter;

    @RequestMapping("/")
    public String greeting() {
        return "index";
    }

    @MessageMapping("/search")
    public void search(String query) {
        twitter.search(query);
    }
}
