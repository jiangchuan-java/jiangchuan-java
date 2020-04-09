package com.ifeng.rocketmq.connect.core;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
public class ConnectMessageListener implements MessageListenerConcurrently {

    private static final Logger logger = LoggerFactory.getLogger(ConnectMessageListener.class);

    private ConsumerWorker consumerWorker;

    public ConnectMessageListener(ConsumerWorker consumerWorker){
        this.consumerWorker = consumerWorker;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for(MessageExt msg : msgs){
            System.out.printf("%s Receive New Messages: %s %n", System.currentTimeMillis()/1000, new String(msg.getBody()));
            logger.info("sinkUrl: {}, contentType: {}", consumerWorker.getSinkUrl(), consumerWorker.getContentType());
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}
