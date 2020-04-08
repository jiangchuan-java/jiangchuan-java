package com.ifeng.rocketmq.connect.core;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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


    public ConsumerWorker(String workerName, String groupName, String nameSrv, String topic, String tag) throws Exception{
        this.workerName = workerName;
        this.consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(nameSrv);
        this.topic = topic;
        consumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(new DelayTimeHook(consumer));
        consumer.registerMessageListener(new ConnectMessageListener());
        consumer.subscribe(topic,tag);
    }


    public void start() throws Exception{
        consumer.start();
    }

    public void pause(){
        consumer.suspend();
    }

    public void resume() {
        consumer.resume();
    }

    public void shutdown() {
        consumer.shutdown();
    }
}
