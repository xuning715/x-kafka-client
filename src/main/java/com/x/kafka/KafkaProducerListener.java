package com.x.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.kafka.support.ProducerListener;

@SuppressWarnings("rawtypes")
public class KafkaProducerListener implements ProducerListener {
    private static Logger logger = LogManager.getLogger(KafkaProducerListener.class);

    /**
     * 发送消息成功后调用
     */
    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        logger.info("----------topic:" + topic);
        logger.info("----------partition:" + partition);
        logger.info("----------key:" + key);
        logger.info("----------value:" + value);
        logger.info("----------RecordMetadata:" + recordMetadata);
        logger.info("~~~~~~~~~~kafka is success to send message~~~~~~~~~~");
    }

    /**
     * 发送消息错误后调用
     */
    @Override
    public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
        logger.error("----------topic:" + topic);
        logger.error("----------partition:" + partition);
        logger.error("----------key:" + key);
        logger.error("----------value:" + value);
        logger.error("----------Exception:" + exception);
        logger.error("~~~~~~~~~~kafka is fail to send message~~~~~~~~~~");
        logger.error(exception);
    }

    /**
     * 方法返回值代表是否启动kafkaProducer监听器
     */
    @Override
    public boolean isInterestedInSuccess() {
        logger.info("///kafkaProducer listener start success///");
        return true;
    }

}