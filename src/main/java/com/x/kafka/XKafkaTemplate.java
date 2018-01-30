package com.x.kafka;

import com.alibaba.fastjson.JSON;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Map;

public class XKafkaTemplate {
    private DefaultKafkaProducerFactory producerFactory;
    private KafkaProducerListener producerListener = new KafkaProducerListener();
    private KafkaTemplate kafkaTemplate;

    public XKafkaTemplate(Map<String, Object> producerProperties, boolean autoFlush) {
        producerFactory = new DefaultKafkaProducerFactory(producerProperties);
        kafkaTemplate = new KafkaTemplate(producerFactory, autoFlush);
        kafkaTemplate.setDefaultTopic("defaultTopic");
        kafkaTemplate.setProducerListener(producerListener);
    }

    public <T extends Object> ListenableFuture<SendResult<String, String>> produceMessage(String topic, T payload) {
        String json = JSON.toJSONString(payload);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, json);
        return listenableFuture;
    }

    public <T> ListenableFuture<SendResult<String, String>> produceMessage(String topic, Integer partition, String key, T payload) {
        String json = JSON.toJSONString(payload);
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, partition, key, json);
        return listenableFuture;
    }


}
