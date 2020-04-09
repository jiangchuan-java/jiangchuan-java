package com.ifeng.rocketmq.connect.core;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
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
 * @Date: 20-4-8
 */
public class ConsumerWorker {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

    private String workerName;

    private String workerId;

    private String topic;

    private DefaultMQPushConsumer consumer;

    private ConnectMessageListener connectMessageListener;

    private volatile String sinkUrl;

    private volatile String contentType;

    private volatile boolean hasStarted = false;


    public ConsumerWorker(String workerName, String groupName, String nameSrv,
                          String topic, ApolloConfigModel config) throws Exception{
        this.workerName = workerName;
        this.consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(nameSrv);
        this.topic = topic;
        consumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(new DelayTimeHook(consumer));
        consumer.registerMessageListener(new ConnectMessageListener(this));
        consumer.subscribe(topic,workerName);
    }

    public void updateConfig(String sinkUrl, String contentType){
        this.sinkUrl = sinkUrl;
        this.contentType = contentType;
    }


    public void start() throws Exception{
        consumer.start();
        hasStarted = true;
        logger.info("{} start completed", workerName);
    }

    public void pause() throws Exception{
        consumer.suspend();
        logger.info("{} suspend completed", workerName);
    }

    public void resume() throws Exception{
        consumer.resume();
        if(!hasStarted){
            start();
        }
        logger.info("{} resume completed", workerName);
    }

    public void shutdown() throws Exception{
        consumer.shutdown();
        logger.info("{} shutdown completed", workerName);
    }


    public String getContentType() {
        return contentType;
    }

    public String getSinkUrl() {
        return sinkUrl;
    }
}
