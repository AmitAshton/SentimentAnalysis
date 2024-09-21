package com.ashton.SentimentAnalysis.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.UUID;

@Component
public class AppKafkaSender {

    @Autowired
    @Qualifier("simpleProducer")
    private KafkaSender<String, String> kafkaSender;

    public boolean send(String msg, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, UUID.randomUUID().toString(),
                msg);
        Mono<SenderRecord<String, String, String>> mono = Mono.just(SenderRecord.create(record, null));
        Flux<SenderResult<String>> res =  kafkaSender.send(mono);
        res.collectList().subscribe();

        return true;
    }
}
