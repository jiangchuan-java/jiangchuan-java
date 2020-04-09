package com.ifeng.rocketmq.connect.core;

import com.ifeng.rocketmq.connect.util.HttpUtils;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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

    public ConnectMessageListener(ConsumerWorker consumerWorker) {
        this.consumerWorker = consumerWorker;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            //本条消息剩余重试次数
            int lastRemainRetries = consumerWorker.getRemainRetries();
            String url = consumerWorker.getSinkUrl();
            String contentType = consumerWorker.getContentType();
            String params = new String(msg.getBody());
            long retryInterval = consumerWorker.getRetryInertval();

            logger.info("sinkUrl: {}, contentType: {}, params: {}", url, contentType, params);

            String response;
            while (lastRemainRetries > 0) {
                try {
                    long requestStart = System.currentTimeMillis();
                    response = HttpUtils.post(url, params, "UTF-8", Collections.singletonMap("Content-Type", contentType), 4000, 4000, 500);
                    long requestEnd = System.currentTimeMillis();
                    logger.info("http sink request(singleSink) cost(ms): {} url: {}, resp: {}", (requestEnd - requestStart), url, response);
                    break;
                } catch (Exception e) {
                    lastRemainRetries--;
                    logger.error("sink过程出现异常进行重试 url: {},lastRemainRetries: {}, exception: {}", url, lastRemainRetries, e);
                    try {
                        Thread.sleep(retryInterval);
                    }catch (Exception e2){
                        logger.error("重试等待出现异常 url: {},retryInterval: {}, exception: {}", url, retryInterval, e);
                    }
                }
            }

        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}
