package com.github.lisama569.localmessagequeuejava.mq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地消息队列配置。
 */
@ConfigurationProperties(prefix = "mq")
public class MqProperties {

    /** 是否启用内置门面服务 */
    private boolean enabled = false;
    /** 数据目录（消息日志、位点、死信、去重集合） */
    private String dataDir = "./mq-data";
    /** 每分区容量上限 */
    private long perPartitionCapacity = 10_000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public long getPerPartitionCapacity() {
        return perPartitionCapacity;
    }

    public void setPerPartitionCapacity(long perPartitionCapacity) {
        this.perPartitionCapacity = perPartitionCapacity;
    }
}
