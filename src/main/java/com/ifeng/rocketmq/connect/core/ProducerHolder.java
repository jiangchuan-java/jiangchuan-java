package com.ifeng.rocketmq.connect.core;


import com.ifeng.rocketmq.connect.config.ConnectInnerProperties;
import com.ifeng.rocketmq.connect.config.ConnectMsgProperties;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
public class ProducerHolder{

    private static final Logger logger = LoggerFactory.getLogger(ProducerHolder.class);

    private DefaultMQProducer defaultMQProducer;

    private ConnectInnerProperties properties;


    public ProducerHolder(ConnectInnerProperties properties) {
        try {
            this.properties = properties;
            this.defaultMQProducer = new DefaultMQProducer(this.properties.groupName);
            this.defaultMQProducer.getDefaultMQProducerImpl().registerCheckForbiddenHook(new DelayTimeHook(this.defaultMQProducer));
            this.defaultMQProducer.setNamesrvAddr(properties.nameSrv);
            this.defaultMQProducer.start();
            logger.info("****** ProducerHolder init completed ******");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public SendResult sendMsg(String tag, String body, Integer delayTimeSeconds) {
        try {
            Message msg = new Message(this.properties.topic, tag, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            if (Objects.nonNull(delayTimeSeconds) && delayTimeSeconds > 0) {
                msg.putUserProperty(ConnectMsgProperties.CUSTOM_DELAY_SECONDS, String.valueOf(delayTimeSeconds));
            }
            SendResult sendResult = defaultMQProducer.send(msg);
            return sendResult;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public ConnectInnerProperties getProperties() {
        return properties;
    }

    public void setProperties(ConnectInnerProperties properties) {
        this.properties = properties;
    }
}
