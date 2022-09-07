package org.rainy.schedule.config;

import org.rainy.schedule.lock.RedisScheduleLock;
import org.rainy.schedule.lock.ScheduleLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author zhangyu
 * create at 2022/9/7 0007 14:42
 */
@Configuration
public class ScheduleConfig {

    private final RedisTemplate<String, String> redisTemplate;

    public ScheduleConfig(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Bean
    @ConditionalOnMissingBean(value = ScheduleLock.class)
    public ScheduleLock getScheduleLock() {
        return new RedisScheduleLock(redisTemplate);
    }

}
