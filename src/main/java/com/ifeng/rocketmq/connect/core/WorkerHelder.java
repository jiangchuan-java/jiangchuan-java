package com.ifeng.rocketmq.connect.core;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;

import com.ifeng.rocketmq.connect.config.ApolloConfigModel;
import com.ifeng.rocketmq.connect.config.ConnectInnerProperties;
import com.ifeng.rocketmq.connect.config.WorkerStateConstant;
import com.ifeng.rocketmq.connect.util.JackSonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-8
 */
public class WorkerHelder {

    private static final Logger logger = LoggerFactory.getLogger(WorkerHelder.class);

    private ExecutorService executorService;

    private Config config;

    private ConcurrentHashMap<String, ConsumerWorker> workerMap;

    private ConnectInnerProperties properties;

    public WorkerHelder(ConnectInnerProperties properties) throws Exception{
        this.properties = properties;
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        config = ConfigService.getAppConfig();
        workerMap = new ConcurrentHashMap<>(16);
        consumerInit();
        logger.info("****** WorkerHelder init completed ******");
    }

    private void consumerInit() {
        Set<String> set = config.getPropertyNames();
        for (String workerName : set) {
            String configValue = config.getProperty(workerName, null);
            if (Objects.nonNull(configValue)) {
                try {
                    ApolloConfigModel apolloConfig = JackSonUtils.json2Bean(configValue, ApolloConfigModel.class);
                    if (apolloConfig.getState() != WorkerStateConstant.DELETE) {
                        buildConsumerWorker(workerName, apolloConfig);
                    }
                } catch (Throwable t) {
                    logger.error("consumerInit failed : {}", t);
                }
            }
        }
        config.addChangeListener(new WorkerStateChangeListener());
    }


    private void buildConsumerWorker(String workerName, ApolloConfigModel apolloConfig) {
        try {
            ConsumerWorker consumerWorker = new ConsumerWorker(workerName, properties.groupName,
                    properties.nameSrv, properties.topic,apolloConfig);
            logger.info("{} build success", workerName);
            workerMap.put(workerName, consumerWorker);
            stateChangeHandler(workerName, apolloConfig.getState());
        } catch (Exception e) {
            logger.error("buildConsumerWorker failed workerName: {}, exception: {}", workerName, e);
        }
    }

    private class WorkerStateChangeListener implements ConfigChangeListener {

        @Override
        public void onChange(ConfigChangeEvent changeEvent) {
            try {
                for (String workerName : changeEvent.changedKeys()) {
                    String newValue = changeEvent.getChange(workerName).getNewValue();
                    String oldValue = changeEvent.getChange(workerName).getOldValue();

                    ApolloConfigModel newConfig = JackSonUtils.json2Bean(newValue, ApolloConfigModel.class);
                    ApolloConfigModel oldConfig = JackSonUtils.json2Bean(oldValue, ApolloConfigModel.class);
                    if (workerMap.containsKey(workerName)) {
                        diff(workerName, newConfig, oldConfig);
                    } else {
                        buildConsumerWorker(workerName, newConfig);
                    }
                }
            } catch (Exception e) {
                logger.error("WorkerStateChangeListener failed exception: {}", e);
            }
        }
    }

    private void diff(String workerName, ApolloConfigModel newConfig, ApolloConfigModel oldConfig){
        String oldState = oldConfig.getState();
        String newState = newConfig.getState();
        //state has changed
        if(!Objects.equals(oldState,newState)){
            if(oldState.equals(WorkerStateConstant.DELETE)){
                workerMap.remove(workerName);
                buildConsumerWorker(workerName,newConfig);
            } else {
                if(oldState.equals(WorkerStateConstant.PAUSE)){
                    if(newState.equals(WorkerStateConstant.RUNNING) || newState.equals(WorkerStateConstant.RESUME)){
                        newState = WorkerStateConstant.RESUME;
                    }
                }
                stateChangeHandler(workerName,newState);
            }
        }
        configChangeHandler(workerName, newConfig);
    }

    private void stateChangeHandler(String workerName, String state) {
        try {
            ConsumerWorker worker = workerMap.get(workerName);
            switch (state) {
                case WorkerStateConstant.RUNNING:
                    worker.start();
                    break;
                case WorkerStateConstant.RESUME:
                    worker.resume();
                    break;
                case WorkerStateConstant.PAUSE:
                    worker.pause();
                    break;
                case WorkerStateConstant.DELETE:
                    worker.shutdown();
                    break;
            }

        } catch (Throwable t) {
            logger.error("stateChangeHandler failed workerName: {}, state: {}, exception: {}", workerName, state, t);
        }
    }

    private void configChangeHandler(String workerName, ApolloConfigModel config){
        try {
            ConsumerWorker worker = workerMap.get(workerName);
            worker.updateConfig(config.getHttpSinkUrl(),config.getContentType());
        } catch (Throwable t) {
            logger.error("configChangeHandler failed workerName: {}, exception: {}", workerName, t);
        }
    }

    public ConnectInnerProperties getProperties() {
        return properties;
    }

    public void setProperties(ConnectInnerProperties properties) {
        this.properties = properties;
    }
}
