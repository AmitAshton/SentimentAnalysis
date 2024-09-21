package com.ashton.SentimentAnalysis.controller;

import com.ashton.SentimentAnalysis.kafka.AppKafkaSender;
import com.ashton.SentimentAnalysis.news.AppNewsStream;
import com.ashton.SentimentAnalysis.nlp.SentimentAnalyzer;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;

import static com.ashton.SentimentAnalysis.kafka.KafkaTopicConfig.APP_TOPIC;

@RestController
public class AppController {
    @Autowired
    SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    AppNewsStream appNewsStream;

    @Autowired
    AppKafkaSender kafkaSender;

    @Autowired
    KafkaReceiver<String,String> kafkaReceiver;

    @RequestMapping(path = "/stopNews", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> stop()  {
        appNewsStream.shutdown();
        return Mono.just("shutdown");
    }

    public static <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return  source.reduce(new ArrayList(), (a, b) -> { a.add(b);return a; });
    }

    @RequestMapping(path = "/grouped", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> grouped(@RequestParam(defaultValue = "Trump") String text,
                                               @RequestParam(defaultValue = "3") Integer timeWindowSec) throws InterruptedException {
        var flux = kafkaReceiver.receive().map(message -> message.value());
        appNewsStream.filter(text).map((x)-> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x-> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window->toArrayList(window))
                .map(y->{
                    if (y.size() == 0) return "size: 0 <br>";
                    return  "time:" + y.get(0).curTime +  " size: " + y.size() + "<br>";
                });
    }

    @RequestMapping(path = "/sentiment", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> sentiment(@RequestParam(defaultValue = "Trump") String text,
                                                 @RequestParam(defaultValue = "3") Integer timeWindowSec) throws InterruptedException {
        var flux = kafkaReceiver.receive().map(message -> message.value());
        appNewsStream.filter(text).map((x)-> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x-> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window->toArrayList(window))
                .map(items->{
                    if (items.size() > 10) return "size:" + items.size() + "<br>";
                    System.out.println("size:" + items.size());
                    double avg = items.stream().map(x-> sentimentAnalyzer.analyze(x.message))
                            .mapToDouble(y->y).average().orElse(0.0);
                    if (items.size() == 0) return "EMPTY<br>";
                    return   items.size() + " messages, sentiment = " + avg +  "<br>";

                });
    }

    static class TimeAndMessage {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss, z");
        DateTime curTime;
        String message;

        public TimeAndMessage(DateTime curTime, String message) {
            this.curTime = curTime;
            this.message = message;
        }

        @Override
        public String toString() {
            return "TimeAndMessage{" +
                    "formatter=" + formatter +
                    ", curTime=" + curTime +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
}

