package com.realtime.kafka.admin;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.realtime.common.Constants.ZK_SERVERS;

public class KafkaAdminUtil {

    private static Log log = LogFactory.getLog(KafkaAdminUtil.class);
    private static final int ZK_TIMEOUT_MSEC =
            (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);


    public void createTopic(String zkServers,
                            String topic,
                            int partitions,
                            Properties topicProperties) {
        ZkUtils zkUtils = ZkUtils.apply(ZK_SERVERS, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
        try {
            if (AdminUtils.topicExists(zkUtils, topic)) {
                log.info(String.format("No need to create topic %s as it already exists", topic));
            } else {
                log.info(String.format("Creating topic %s with %s partition(s)", topic, partitions));
                try {
                    AdminUtils.createTopic(
                            zkUtils, topic, partitions, 1,
                            topicProperties, RackAwareMode.Enforced$.MODULE$);
                    log.info(String.format("Created topic %s", topic));
                } catch (Exception re) {
                    log.info(String.format("Topic %s already exists", topic));
                }
            }
        } finally {
            zkUtils.close();
        }
    }
}
