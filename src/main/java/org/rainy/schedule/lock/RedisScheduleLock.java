package org.rainy.schedule.lock;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * @author zhangyu
 * create at 2022/9/6 0006 16:28
 */
@Component
public class RedisScheduleLock implements ScheduleLock {

    private final RedisTemplate<String, String> redisTemplate;

    public RedisScheduleLock(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean lock(String taskId, String appId, Duration timeout) {
        System.out.println("timeout: " + timeout.toMillis());
        Boolean result = redisTemplate.opsForValue().setIfAbsent(taskId, appId, timeout);
        return result == null ? Objects.equals(redisTemplate.opsForValue().get(taskId), appId) : result;  
    }

    @Override
    public boolean updateExpire(String taskId, Duration timeout) {
        String val = redisTemplate.opsForValue().get(taskId);
        System.out.println("updateExpire: "+val);
        
        return Optional.ofNullable(redisTemplate.expire(taskId, timeout)).orElse(Boolean.FALSE);
    }

}
