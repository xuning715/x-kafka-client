package com.x.kafka;

import java.lang.reflect.Method;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class KafkaConsumerListener implements MessageListener<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerListener.class);

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    private Object service;

    private String methodName;

    private String paramType;

    public KafkaConsumerListener(ThreadPoolTaskExecutor threadPoolTaskExecutor, Object service, String methodName, String paramType) {
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
        this.service = service;
        this.methodName = methodName;
        this.paramType = paramType;
    }

    @Override
    public void onMessage(final ConsumerRecord<String, String> record) {
        threadPoolTaskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                //根据不同主题，消费
                String message = record.value();
                logger.info("Consumertopic:" + record.topic());
                logger.info("Consumervalue:" + message);
                logger.info("Consumerkey:" + record.key());
                logger.info("Consumeroffset:" + record.offset());
                logger.info("Consumerpartition:" + record.partition());
                logger.info("ConsumertimestampType:" + record.timestampType());
                logger.info("---------------------------------------------------------");
                try {
                    Class clazz = Class.forName(paramType);
                    Object param = JSON.parseObject(message, clazz);
                    Method method = service.getClass().getMethod(methodName, clazz);
                    method.invoke(service, param);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        });
    }

    @KafkaListener(topics = {"ERP_CAR_STATE_2"}, containerFactory = "consumerFactory")
    public void listenErpCarState2(ConsumerRecord<String, String> record) {
        logger.info("ERP_CAR_STATE_2");
        logger.info("topic:" + record.topic());
        logger.info("value:" + record.value());
        logger.info("key:" + record.key());
        logger.info("offset:" + record.offset());
        logger.info("partition:" + record.partition());
        logger.info("timestampType:" + record.timestampType());
        logger.info("---------------------------------------------------------");
//        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
//        if (kafkaMessage.isPresent()) {
//            Object message = kafkaMessage.get();
//            logger.info("message is {} ", message);
//        }
    }
}
