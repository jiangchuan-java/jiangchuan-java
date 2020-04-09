package com.ifeng.rocketmq.connect.core;

import com.ifeng.rocketmq.connect.config.ApolloConfigModel;
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

    //与apollo中的key相对应
    private String workerName;

    private String workerId;

    //topic="ConnectTopic"
    private String topic;

    private DefaultMQPushConsumer consumer;

    private volatile String sinkUrl;

    private volatile String contentType;

    //worker 是否启动过，当初始state=pause时，重启服务，并收到running操作时，要先执行resume，在执行start
    private volatile boolean hasStarted = false;

    //worker 任务推送的重试次数
    private int remainRetries;

    //重试间隔
    private long retryInertval;


    public ConsumerWorker(String workerName, String groupName, String nameSrv,
                          String topic, ApolloConfigModel config) throws Exception{
        this.workerName = workerName;
        this.consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(nameSrv);
        this.topic = topic;
        consumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(new DelayTimeHook(consumer));
        consumer.registerMessageListener(new ConnectMessageListener(this));
        consumer.subscribe(topic,workerName);
        this.sinkUrl = config.getHttpSinkUrl();
        this.contentType = config.getContentType();
        this.remainRetries = config.getRemainRetries();
        this.retryInertval = config.getRetryInertval();
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

    public int getRemainRetries() {
        return remainRetries;
    }

    public long getRetryInertval() {
        return retryInertval;
    }
}
