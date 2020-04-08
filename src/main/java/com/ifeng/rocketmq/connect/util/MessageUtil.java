package com.ifeng.rocketmq.connect.util;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
public class MessageUtil {

    private static final Logger log = LoggerFactory.getLogger(MessageUtil.class);

    public static HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();

    private static final ConcurrentMap<Integer, Long> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);

    private static final ConcurrentSkipListMap<Long, Integer> delayLevelTableFlip = new ConcurrentSkipListMap<>();
    static {
        timeUnitTable.put("seconds", 1000L);
        timeUnitTable.put("minutes", 1000L * 60);
        timeUnitTable.put("hours", 1000L * 60 * 60);
        timeUnitTable.put("days", 1000L * 60 * 60 * 24);
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
    }

    public static long getBrokerTime(MQClientAPIImpl admin, String brokerAddr){
        try {
            KVTable kvTable = admin.getBrokerRuntimeInfo(brokerAddr,1000);
            Map<String, String> map = kvTable.getTable();
            String bootTimestamp = map.get("bootTimestamp");
            String runtime = map.get("runtime");
            runtime = runtime.replace("[","").replace("]","");
            String[] str = runtime.split(",");
            long brokerTime = Long.valueOf(bootTimestamp);
            for(String s : str){
                String[] item = s.split(" ");
                long num = Long.valueOf(item[1])*timeUnitTable.get(item[2]);
                brokerTime = brokerTime + num;
            }
            return brokerTime;
        }catch (Exception e){
            e.printStackTrace();
            return 0L;
        }
    }

    public static int getDelayTimeLevel(MQClientAPIImpl admin, String brokerAddr, int delaySeconds){
        try {
            Properties properties = admin.getBrokerConfig(brokerAddr,1000);
            String messageDelayLevel = properties.getProperty("messageDelayLevel");
            parseDelayLevel(messageDelayLevel);
            long delayTime = delaySeconds*1000;
            int level = delayLevelTableFlip.floorEntry(delayTime).getValue();
            return level;

        }catch (Exception e){
            e.printStackTrace();
            return 1;
        }
    }

    private static boolean parseDelayLevel(String messageDelayLevel) {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        try {
            String[] levelArray = messageDelayLevel.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                delayLevelTable.put(level, delayTimeMillis);
                delayLevelTableFlip.put(delayTimeMillis, level);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", messageDelayLevel);
            return false;
        }

        return true;
    }
}
