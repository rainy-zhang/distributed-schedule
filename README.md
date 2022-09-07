# distributed-schedule
基于SpringBoot-Scheduling实现的分布式定时任务

其实思路很简单，就是通过AOP拦截@Scheduled注解，并尝试对任务添加分布式锁，加锁失败则拦截该任务，加锁成功则继续执行。

默认的分布式锁实现是通过Redis实现的，可以参考`RedisScheduleLock`，也可以继承`ScheduleLock`接口自定义分布式锁。
