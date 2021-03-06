var subscriptionTweets = null;
var subscriptionTopics = null;
var newQuery = 0;

function registerTemplate() {
    template = $("#template").html();
    Mustache.parse(template);

    templateTopics = $("#templateTopics").html();
    Mustache.parse(templateTopics);
}

function setConnected(connected) {
    var search = $('#submitsearch');
    search.prop('disabled', !connected);
}

function registerSendQueryAndConnect() {
    var socket = new SockJS("/twitter");
    var stompClient = Stomp.over(socket);
    stompClient.connect({}, function (frame) {
        setConnected(true);
        console.log('Connected: ' + frame);

        subscriptionTopics = stompClient.subscribe("/queue/trends", function (data) {

            var resultsBlock = $("#resultsTopics");
            var topics = JSON.parse(data.body);

            console.log(topics);

            resultsBlock.empty();
            for (var i in topics) {
                for (var topic in topics[i]) {
                    resultsBlock.append("<li class=\"list-group-item\">"+topic+"("+topics[i][topic]+")</li>");
                }
            }
        });

    });
    $("#search").submit(
            function (event) {
                event.preventDefault();
                if (subscriptionTweets) {
                    subscriptionTweets.unsubscribe();
                }
                if (subscriptionTopics) {
                    subscriptionTopics.unsubscribe();
                }
                var query = $("#q").val();
                stompClient.send("/app/search", {}, query);
                newQuery = 1;
                subscriptionTweets = stompClient.subscribe("/queue/search/" + query, function (data) {
                    var resultsBlock = $("#resultsBlock");
                    if (newQuery) {
                        resultsBlock.empty();
                        newQuery = 0;
                    }
                    var tweet = JSON.parse(data.body);
                    resultsBlock.prepend(Mustache.render(template, tweet));
                });
            });
}

$(document).ready(function () {
    registerTemplate();
    registerSendQueryAndConnect();
});
