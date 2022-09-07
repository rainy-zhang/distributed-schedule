package org.rainy.schedule.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.rainy.schedule.lock.ScheduleLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author zhangyu
 * create at 2022/9/6 0006 15:38
 */
@Aspect
@Component
public class ScheduleAspect {

    @Value(value = "${spring.application.name}")
    private String appId;

    private final Map<Scheduled, LocalDateTime> timeouts = new HashMap<>();
    private long fixedDelay;
    private String fixedDelayString;
    private String key;

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
        LocalDateTime now = LocalDateTime.now();
        Class<?> targetClass = pjp.getTarget().getClass();
        // 获取方法签名
        MethodSignature methodSignature = (MethodSignature) pjp.getSignature();
        Method method = targetClass.getDeclaredMethod(methodSignature.getName(), methodSignature.getParameterTypes());
        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        // 完整的方法名作为任务ID
        String taskId = method.toGenericString();


        // 获取定时任务间隔时间，作为锁的超时时间
        LocalDateTime next = getTimeout(scheduled);

        if (next == null) {
            // next == null说明任务间隔时间是以执行完毕后的时间点为准，先把fixedDelay指定的时间作为锁时间，任务执行完毕后再更新锁时间 
            next = now.plus(getFixedDelay(), ChronoUnit.MILLIS);
        }
        Duration timeout = Duration.between(now, next);

        // 时间取整，比如每10秒执行一次的任务，对其当前执行时间中的秒数取整
        LocalDateTime roundText = roundTaskTime(now, timeout.toMillis());
        this.key = String.format("%s:%s", roundText.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), taskId);
        if (!scheduleLock.lock(key, appId, timeout)) {
            System.out.println("redis not empty: " + key);
            return;
        }
        pjp.proceed();
    }

    @After("schedulePoint()")
    public void after() {
        Duration timeout = Duration.ofMillis(getFixedDelay());
        if (timeout.toMillis() > 0) {
            if (!scheduleLock.updateExpire(key, timeout)) {
                System.err.println("update expire failed, key: " + key);
            }
        }
    }

    private long getFixedDelay() {
        long timeout = 0;
        if (fixedDelay > -1) {
            timeout = fixedDelay;
        } else if (!fixedDelayString.isEmpty()) {
            // 处理占位符
            if (fixedDelayString.contains("$")) {
                fixedDelayString = Optional.ofNullable(environment.getProperty(fixedDelayString.substring(fixedDelayString.indexOf("{") + 1, fixedDelayString.indexOf("}")))).orElseThrow(() -> new RuntimeException(fixedDelayString + "not found"));
            }
            timeout = Long.parseLong(fixedDelayString);
        }
        return timeout;
    }

    private LocalDateTime getTimeout(Scheduled scheduled) {
        return timeouts.computeIfAbsent(scheduled, k -> {
            // 因为fixedDelay和fixedDelayString是任务执行结束后的时间间隔，所以需要任务执行完毕后再加锁
            this.fixedDelay = scheduled.fixedDelay();
            this.fixedDelayString = scheduled.fixedDelayString();

            LocalDateTime now = LocalDateTime.now();

            String cron = scheduled.cron();
            long fixedRate = scheduled.fixedRate();
            String fixedRateString = scheduled.fixedRateString();

            if (!cron.isEmpty()) {
                return CronExpression.parse(cron).next(now);
            }
            if (fixedRate > -1) {
                return now.plus(fixedRate, ChronoUnit.MILLIS);
            }
            if (!fixedRateString.isEmpty()) {
                // 处理占位符
                if (fixedRateString.contains("$")) {
                    String finalFixedRateString = fixedRateString;
                    fixedRateString = Optional.ofNullable(environment.getProperty(fixedRateString.substring(fixedRateString.indexOf("{") + 1, fixedRateString.indexOf("}")))).orElseThrow(() -> new RuntimeException(finalFixedRateString + "not found"));
                }
                return now.plus(Long.parseLong(fixedRateString), ChronoUnit.MILLIS);
            }
            return null;
        });
    }

    private LocalDateTime roundTaskTime(LocalDateTime now, long intervalMillis) {
        long secondMillis = 1000;
        long minuteMillis = 60 * secondMillis;
        long hourMillis = 60 * minuteMillis;
        long dayMillis = 24 * hourMillis;
        long monthMillis = 30 * dayMillis;

        if (intervalMillis >= monthMillis) {
            int month = now.getMonthValue();
            long intervalMonth = (intervalMillis / monthMillis);
            return now.withMonth((int) ((month / intervalMonth) * intervalMonth));
        }
        if (intervalMillis >= dayMillis) {
            int day = now.getDayOfMonth();
            long intervalDay = (intervalMillis / dayMillis);
            return now.withDayOfMonth((int) ((day / intervalDay) * intervalDay));
        }
        if (intervalMillis >= hourMillis) {
            int hour = now.getHour();
            long intervalHour = (intervalMillis / hourMillis);
            return now.withHour((int) ((hour / intervalHour) * intervalHour));
        }
        if (intervalMillis >= minuteMillis) {
            int minute = now.getMinute();
            long intervalMinute = (intervalMillis / minuteMillis);
            return now.withMinute((int) ((minute / intervalMinute) * intervalMinute));
        }
        if (intervalMillis >= secondMillis) {
            int second = now.getSecond();
            long intervalSecond = (intervalMillis / secondMillis);
            return now.withSecond((int) ((second / intervalSecond) * intervalSecond));
        }

        return now;
    }


}
