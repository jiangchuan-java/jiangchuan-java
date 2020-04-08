package com.ifeng.rocketmq.connect.core;

import com.ifeng.rocketmq.connect.config.ConnectMsgProperties;
import com.ifeng.rocketmq.connect.util.MessageUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
public class DelayTimeHook implements CheckForbiddenHook, ConsumeMessageHook {

    private static Logger log = LoggerFactory.getLogger(DelayTimeHook.class);

    private DefaultMQPushConsumer defaultMQPushConsumer;

    private DefaultMQProducer defaultMQProducer;

    public DelayTimeHook(DefaultMQProducer producer){
        this.defaultMQProducer = producer;
    }

    public DelayTimeHook(DefaultMQPushConsumer consumer){
        this.defaultMQPushConsumer = consumer;
    }
    @Override
    public String hookName() {
        return "delay-time-hook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        List<MessageExt> msgList = context.getMsgList();
        Iterator<MessageExt> iterator = msgList.iterator();
        while (iterator.hasNext()){
            MessageExt msg = iterator.next();
            String customExecuteTimeStamp = msg.getProperty(ConnectMsgProperties.CUSTOM_EXECUTE_TIMESTAMP);
            if(Objects.nonNull(customExecuteTimeStamp)){
                long timeStamp = Long.valueOf(customExecuteTimeStamp);
                final String brokerAddr = RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                MQClientAPIImpl admin = defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl();
                long brokerTime = MessageUtil.getBrokerTime(admin, brokerAddr);
                long diff = timeStamp - brokerTime;
                if(diff > 0){
                    int delayLevel = MessageUtil.getDelayTimeLevel(admin, brokerAddr,Long.valueOf(diff/1000).intValue());
                    long executeTimeStamp = MessageUtil.getBrokerTime(admin, brokerAddr) + diff;
                    msg.putUserProperty(ConnectMsgProperties.CUSTOM_EXECUTE_TIMESTAMP, String.valueOf(executeTimeStamp));
                    msg.setDelayTimeLevel(delayLevel);
                    try {
                        defaultMQPushConsumer.getDefaultMQPushConsumerImpl().sendMessageBack(msg,delayLevel,null);
                        iterator.remove();
                        log.info("executeTime sendback success, delayLevel : {}, msg : {}", delayLevel, msg);
                    }catch (Throwable t){
                        log.error("executeTime sendback error, msg : " + msg, t);
                    }
                }
            }
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {

    }

    @Override
    public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
        Message msg = context.getMessage();
        String custom_delay_seconds = msg.getProperty(ConnectMsgProperties.CUSTOM_DELAY_SECONDS);
        if(Objects.nonNull(custom_delay_seconds)){
            MQClientAPIImpl admin = defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl();
            String brokerAddr = context.getBrokerAddr();
            int delayLevel = MessageUtil.getDelayTimeLevel(admin, brokerAddr, Integer.valueOf(custom_delay_seconds));
            long executeTimeStamp = MessageUtil.getBrokerTime(admin, brokerAddr) + Long.valueOf(custom_delay_seconds)*1000;
            msg.putUserProperty(ConnectMsgProperties.CUSTOM_EXECUTE_TIMESTAMP, String.valueOf(executeTimeStamp));
            msg.setDelayTimeLevel(delayLevel);
            log.info("executeTime : {}, delay(ms) : {}, message : {}"
                    , new Date(executeTimeStamp), delayLevel, msg);
        }

    }
}
