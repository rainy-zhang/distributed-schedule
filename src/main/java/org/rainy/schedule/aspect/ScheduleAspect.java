package org.rainy.schedule.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.rainy.schedule.lock.ScheduleLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyu
 * create at 2022/9/6 0006 15:38
 */
@Aspect
@Component
public class ScheduleAspect {

    @Value(value = "${spring.application.name}")
    private String appId;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private final Map<Scheduled, Duration> timeouts = new HashMap<>();
    private long fixedDelay;
    private String fixedDelayString;
    private String taskId;

    private final ScheduleLock scheduleLock;
    private final Environment environment;

    public ScheduleAspect(ScheduleLock scheduleLock, Environment environment) {
        this.scheduleLock = scheduleLock;
        this.environment = environment;
    }

    @Pointcut("@annotation(org.springframework.scheduling.annotation.Scheduled)")
    public void schedulePoint() {
    }

    @Around("schedulePoint()")
    public void handleAround(ProceedingJoinPoint pjp) throws Throwable {
        System.out.println("------------------------------");
        System.out.println("start: " + System.currentTimeMillis());
        Class<?> targetClass = pjp.getTarget().getClass();
        // 获取方法签名
        MethodSignature methodSignature = (MethodSignature) pjp.getSignature();
        Method method = targetClass.getDeclaredMethod(methodSignature.getName(), methodSignature.getParameterTypes());
        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        // 完整的方法名作为任务ID
        String taskId = method.toGenericString();
        this.taskId = taskId;

        // 获取定时任务间隔时间，作为锁的超时时间
        Duration timeout = getTimeout(scheduled);

        if (timeout == null) {
            // timeout == null说明任务间隔时间是以执行完毕后的时间点为准，这里先临时把锁时间设置为5秒，等任务执行完毕后再更新锁时间 
            timeout = Duration.ofMillis(5000);
        }

        if (!scheduleLock.lock(taskId, appId, timeout)) {
            System.out.println(appId + "-"+ taskId  +": redis not empty");

            System.out.println("expire: "+redisTemplate.getExpire(taskId, TimeUnit.MILLISECONDS));
            
            return;
        }
        pjp.proceed();
    }

    @After("schedulePoint()")
    public void after() {
        Duration timeout = null;
        if (fixedDelay > -1) {
            timeout = Duration.ofMillis(fixedDelay);
        } else if (!fixedDelayString.isEmpty()) {
            // 处理占位符
            if (fixedDelayString.contains("$")) {
                fixedDelayString = Optional.ofNullable(environment.getProperty(fixedDelayString.substring(fixedDelayString.indexOf("{") + 1, fixedDelayString.indexOf("}")))).orElseThrow(() -> new RuntimeException(fixedDelayString + "not found"));
            }
            timeout = Duration.ofMillis(Long.parseLong(fixedDelayString));
        }

        if (timeout != null) {
            if (scheduleLock.updateExpire(taskId, timeout)) {
                System.out.println(appId + "-"+ taskId  +": update expire");
            }
        }
    }


    private Duration getTimeout(Scheduled scheduled) {
        return timeouts.computeIfAbsent(scheduled, k -> {
            // 因为fixedDelay和fixedDelayString是任务执行结束后的时间间隔，所以需要任务执行完毕后再加锁
            this.fixedDelay = scheduled.fixedDelay();
            this.fixedDelayString = scheduled.fixedDelayString();

            String cron = scheduled.cron();
            long fixedRate = scheduled.fixedRate();
            String fixedRateString = scheduled.fixedRateString();

            if (!cron.isEmpty()) {
                LocalDateTime now = LocalDateTime.now();
                LocalDateTime next = CronExpression.parse(cron).next(now);
                return Duration.between(now, next);
            }
            if (fixedRate > -1) {
                return Duration.ofMillis(fixedRate);
            }
            if (!fixedRateString.isEmpty()) {
                // 处理占位符
                if (fixedRateString.contains("$")) {
                    String finalFixedRateString = fixedRateString;
                    fixedRateString = Optional.ofNullable(environment.getProperty(fixedRateString.substring(fixedRateString.indexOf("{") + 1, fixedRateString.indexOf("}")))).orElseThrow(() -> new RuntimeException(finalFixedRateString + "not found"));
                }
                return Duration.ofMillis(Long.parseLong(fixedRateString));
            }
            return null;
        });
    }

}
