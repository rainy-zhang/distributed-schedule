package org.rainy.schedule.lock;

import java.time.Duration;

/**
 * @author zhangyu
 * create at 2022/9/6 0006 16:06
 */
public interface ScheduleLock {

    boolean lock(String taskId, String appId, Duration timeout);
    
    boolean updateExpire(String taskId, Duration timeout);
}
