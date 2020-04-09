package com.ifeng.rocketmq.connect.route;

import com.alibaba.fastjson.JSONObject;
import com.ifeng.rocketmq.connect.core.ProducerHolder;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
@RestController
public class SendMsgRouteHandler {

    @Autowired
    private ProducerHolder producerHolder;

    @RequestMapping(path = "/sendMsg", method = RequestMethod.POST)
    public String sendMsg(HttpServletRequest request, @RequestBody JSONObject jsonObject){
        String tag = jsonObject.getString("tag");
        String body = jsonObject.getString("body");
        Integer delayTimeSeconds = jsonObject.getInteger("delayTimeSeconds");
        SendResult sendResult = producerHolder.sendMsg(tag,body,delayTimeSeconds);
        return sendResult.toString();
    }


}
