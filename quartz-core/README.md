### Quartz集群数据库表

Quartz的集群部署方案在架构上是分布式的，没有负责集中管理的节点，而是利用数据库锁的方式来实现集群环境下进行并发控制。BTW，分布式部署时需要保证各个节点的系统时间一致。

| 表名 | 描述 |
| --- | --- |
| QRTZ_CALENDARS | 存储Quartz的Calendar信息 |
| QRTZ_CRON_TRIGGERS | 存储CronTrigger，包括Cron表达式和时区信息 |
| QRTZ_FIRED_TRIGGERS | 存储与已触发的Trigger相关的状态信息，以及相联Job的执行信息 |
| QRTZ_PAUSED_TRIGGER_GRPS | 存储已暂停的Trigger组的信息 |
| QRTZ_SCHEDULER_STATE | 存储少量的有关Scheduler的状态信息，和别的Scheduler实例 |
| QRTZ_LOCKS | 存储程序的悲观锁的信息 |
| QRTZ_JOB_DETAILS | 存储每一个已配置的Job的详细信息 |
| QRTZ_JOB_LISTENERS | 存储有关已配置的JobListener的信息 |
| QRTZ_SIMPLE_TRIGGERS | 存储简单的Trigger，包括重复次数、间隔、以及已触的次数 |
| QRTZ_BLOG_TRIGGERS | Trigger作为Blob类型存储 |
| QRTZ_TRIGGER_LISTENERS | 存储已配置的TriggerListener的信息 |
| QRTZ_TRIGGERS | 存储已配置的Trigger的信息 |

其中，`QRTZ_LOCKS`就是`Quartz`集群实现同步机制的行锁表，其表结构如下：
```sql
--QRTZ_LOCKS表结构
CREATE TABLE `QRTZ_LOCKS` (
  `LOCK_NAME` varchar(40) NOT NULL,
   PRIMARY KEY (`LOCK_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--QRTZ_LOCKS记录
+-----------------+ 
| LOCK_NAME       |
+-----------------+ 
| CALENDAR_ACCESS |
| JOB_ACCESS      |
| MISFIRE_ACCESS  |
| STATE_ACCESS    |
| TRIGGER_ACCESS  |
+-----------------+
```

可以看出`QRTZ_LOCKS`中有5条记录，代表5把锁，分别用于实现多个`Quartz Node`对`Job`、`Trigger`、`Calendar`访问的同步控制。

### Quartz线程模型

在Quartz中有两类线程：Scheduler调度线程和任务执行线程。

*任务执行线程*

Quartz不会在主线程(QuartzSchedulerThread)中处理用户的Job。

`Quartz`把线程管理的职责委托给`ThreadPool`，一般的设置使用`SimpleThreadPool`。`SimpleThreadPool`创建了一定数量的`WorkerThread`实例来使得`Job`能够在线程中进行处理。`WorkerThread`是定义在`SimpleThreadPool`类中的内部类，它实质上就是一个线程。

```
<!-- 线程池配置 -->
<prop key="org.quartz.threadPool.class">org.quartz.simpl.SimpleThreadPool</prop>
<prop key="org.quartz.threadPool.threadCount">20</prop>
<prop key="org.quartz.threadPool.threadPriority">5</prop>
```

*QuartzSchedulerThread调度主线程*

`QuartzScheduler`被创建时创建一个`QuartzSchedulerThread`实例。

[quartz源码分析——执行引擎和线程模型](https://www.cnblogs.com/liuroy/p/7517777.html)

[Spring定时任务Quartz执行全过程源码解读](https://my.oschina.net/itstack/blog/4409556)

- SimpleThreadPool——quartz里的工头儿
- WorkerThread——quartz里的工人
- QuartzSchedulerThread——Quartz里面的老板

### Quartz初始化流程

Client -> StdSchedulerFactory -> QuartzSchedulerResources -> QuartzScheduler -> SimpleThreadPool -> QuartzSchedulerThread -> StdScheduler