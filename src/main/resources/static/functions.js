var subscription = null;
var newQuery = 0;

function registerTemplate() {
    template = $("#template").html();
    Mustache.parse(template);
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
    });
    $("#search").submit(
            function (event) {
                event.preventDefault();
                if (subscription) {
                    subscription.unsubscribe();
                }
                var query = $("#q").val();
                stompClient.send("/app/search", {}, query);
                newQuery = 1;
                subscription = stompClient.subscribe("/queue/search/" + query, function (data) {
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
