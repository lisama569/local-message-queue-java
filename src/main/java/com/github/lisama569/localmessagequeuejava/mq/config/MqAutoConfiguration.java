package com.github.lisama569.localmessagequeuejava.mq.config;

import com.github.lisama569.localmessagequeuejava.mq.MqService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

/**
 * 本地消息队列自动装配：mq.enabled=true 时创建门面服务。
 */
@AutoConfiguration
@EnableConfigurationProperties(MqProperties.class)
@ConditionalOnProperty(prefix = "mq", name = "enabled", havingValue = "true")
public class MqAutoConfiguration {

    @Bean(destroyMethod = "close")
    public MqService mqService(MqProperties properties) {
        return new MqService(Path.of(properties.getDataDir()), properties.getPerPartitionCapacity());
    }
}
