package com.ifeng.rocketmq.connect.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-8
 */
public class ConnectInnerProperties {

    private static final Logger logger = LoggerFactory.getLogger(ConnectInnerProperties.class);

    public final String groupName;

    public final String topic;

    public final String nameSrv;

    public ConnectInnerProperties(String groupName, String topic, String nameSrv){
        this.groupName = groupName;
        this.topic = topic;
        this.nameSrv = nameSrv;
        logger.info("****** ConnectInnerProperties init completed ******");
    }
}
