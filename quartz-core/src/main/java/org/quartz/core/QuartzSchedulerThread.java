
/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package org.quartz.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The thread responsible for performing the work of firing <code>{@link Trigger}</code>
 * s that are registered with the <code>{@link QuartzScheduler}</code>.
 * </p>
 *
 * @see QuartzScheduler
 * @see org.quartz.Job
 * @see Trigger
 *
 * @author James House
 */
// Quartz集群同步机制：
// 每当要进行与某种业务相关的数据库操作时，先去QRTZ_LOCKS表中查询操作相关的业务对象所需要的锁，在select语句之后加for update来实现。
// 例如，TRIGGER_ACCESS表示对任务触发器相关的信息进行修改、删除操作时所需要获得的锁。
// 当一个线程使用上述的SQL对表中的数据执行查询操作时，若查询结果中包含相关的行，数据库就对该行进行ROW LOCK；
// 若此时，另外一个线程使用相同的SQL对表的数据进行查询，由于查询出的数据行已经被数据库锁住了，此时这个线程就只能等待，
// 直到拥有该行锁的线程完成了相关的业务操作，执行了commit动作后，数据库才会释放了相关行的锁，这个线程才能继续执行。
public class QuartzSchedulerThread extends Thread {
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    private QuartzScheduler qs;

    private QuartzSchedulerResources qsRsrcs;

    private final Object sigLock = new Object();

    private boolean signaled;
    private long signaledNextFireTime;

    private boolean paused;

    private AtomicBoolean halted;

    private Random random = new Random(System.currentTimeMillis());

    // When the scheduler finds there is no current trigger to fire, how long
    // it should wait until checking again...
    private static long DEFAULT_IDLE_WAIT_TIME = 30L * 1000L;

    private long idleWaitTime = DEFAULT_IDLE_WAIT_TIME;

    private int idleWaitVariablness = 7 * 1000;

    private final Logger log = LoggerFactory.getLogger(getClass());

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Construct a new <code>QuartzSchedulerThread</code> for the given
     * <code>QuartzScheduler</code> as a non-daemon <code>Thread</code>
     * with normal priority.
     * </p>
     */
    QuartzSchedulerThread(QuartzScheduler qs, QuartzSchedulerResources qsRsrcs) {
        this(qs, qsRsrcs, qsRsrcs.getMakeSchedulerThreadDaemon(), Thread.NORM_PRIORITY);
    }

    /**
     * <p>
     * Construct a new <code>QuartzSchedulerThread</code> for the given
     * <code>QuartzScheduler</code> as a <code>Thread</code> with the given
     * attributes.
     * </p>
     */
    QuartzSchedulerThread(QuartzScheduler qs, QuartzSchedulerResources qsRsrcs, boolean setDaemon, int threadPrio) {
        super(qs.getSchedulerThreadGroup(), qsRsrcs.getThreadName());
        this.qs = qs;
        this.qsRsrcs = qsRsrcs;
        this.setDaemon(setDaemon);
        if(qsRsrcs.isThreadsInheritInitializersClassLoadContext()) {
            log.info("QuartzSchedulerThread Inheriting ContextClassLoader of thread: " + Thread.currentThread().getName());
            this.setContextClassLoader(Thread.currentThread().getContextClassLoader());
        }

        this.setPriority(threadPrio);

        // start the underlying thread, but put this object into the 'paused'
        // state
        // so processing doesn't start yet...
        paused = true;
        // 初始化任务为正常状态
        halted = new AtomicBoolean(false);
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    void setIdleWaitTime(long waitTime) {
        idleWaitTime = waitTime;
        idleWaitVariablness = (int) (waitTime * 0.2);
    }

    private long getRandomizedIdleWaitTime() {
        return idleWaitTime - random.nextInt(idleWaitVariablness);
    }

    /**
     * <p>
     * Signals the main processing loop to pause at the next possible point.
     * </p>
     */
    void togglePause(boolean pause) {
        synchronized (sigLock) {
            paused = pause;

            if (paused) {
                signalSchedulingChange(0);
            } else {
                sigLock.notifyAll();
            }
        }
    }

    /**
     * <p>
     * Signals the main processing loop to pause at the next possible point.
     * </p>
     */
    void halt(boolean wait) {
        synchronized (sigLock) {
            halted.set(true);

            if (paused) {
                sigLock.notifyAll();
            } else {
                signalSchedulingChange(0);
            }
        }
        
        if (wait) {
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        join();
                        break;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    boolean isPaused() {
        return paused;
    }

    /**
     * <p>
     * Signals the main processing loop that a change in scheduling has been
     * made - in order to interrupt any sleeping that may be occuring while
     * waiting for the fire time to arrive.
     * </p>
     *
     * @param candidateNewNextFireTime the time (in millis) when the newly scheduled trigger
     * will fire.  If this method is being called do to some other even (rather
     * than scheduling a trigger), the caller should pass zero (0).
     */
    public void signalSchedulingChange(long candidateNewNextFireTime) {
        synchronized(sigLock) {
            signaled = true;
            signaledNextFireTime = candidateNewNextFireTime;
            sigLock.notifyAll();
        }
    }

    public void clearSignaledSchedulingChange() {
        synchronized(sigLock) {
            signaled = false;
            signaledNextFireTime = 0;
        }
    }

    public boolean isScheduleChanged() {
        synchronized(sigLock) {
            return signaled;
        }
    }

    public long getSignaledNextFireTime() {
        synchronized(sigLock) {
            return signaledNextFireTime;
        }
    }

    /**
     * <p>
     * The main processing loop of the <code>QuartzSchedulerThread</code>.
     * </p>
     */
    @Override
    public void run() {
        // 这个里面在特殊情况下会与 org.quartz.impl.jdbcjobstore.JobStoreSupport.MisfireHandler 产生锁竞争。
        // QuartzScheduler调度线程不断获取trigger，触发trigger，释放trigger。
        int acquiresFailed = 0;
        // 只有调用了halt()方法，才会退出这个死循环
        while (!halted.get()) {
            try {
                // check if we're supposed to pause...
                synchronized (sigLock) {
                    while (paused && !halted.get()) {
                        try {
                            // wait until togglePause(false) is called...
                            sigLock.wait(1000L);
                        } catch (InterruptedException ignore) {
                        }

                        // reset failure counter when paused, so that we don't
                        // wait again after unpausing
                        acquiresFailed = 0;
                    }

                    if (halted.get()) {
                        break;
                    }
                }

                // wait a bit, if reading from job store is consistently
                // failing (e.g. DB is down or restarting)..
                if (acquiresFailed > 1) {
                    try {
                        long delay = computeDelayForRepeatedErrors(qsRsrcs.getJobStore(), acquiresFailed);
                        Thread.sleep(delay);
                    } catch (Exception ignore) {
                    }
                }

                // blockForAvailableThreads的语义：阻塞直到有空闲的线程可用，然后返回其数量。
                int availThreadCount = qsRsrcs.getThreadPool().blockForAvailableThreads();
                if(availThreadCount > 0) { // will always be true, due to semantics of blockForAvailableThreads...

                    List<OperableTrigger> triggers;

                    long now = System.currentTimeMillis();

                    clearSignaledSchedulingChange();
                    try {
                        // 调度器在trigger队列中寻找30（默认）秒内一定数目的trigger(需要保证集群节点的系统时间一致)
                        // org.quartz.impl.jdbcjobstore.JobStoreSupport.acquireNextTriggers     2798行
                        // org.quartz.scheduler.batchTriggerAcquisitionMaxCount                 Scheduler一次获取trigger的最大数量。默认值为1。
                        // org.quartz.scheduler.batchTriggerAcquisitionFireAheadTimeWindow      允许trigger在调度时间之前获取和触发的时间（单位毫秒）。默认0
                        triggers = qsRsrcs.getJobStore().acquireNextTriggers(
                                now + idleWaitTime, Math.min(availThreadCount, qsRsrcs.getMaxBatchSize()), qsRsrcs.getBatchTimeWindow());
                        acquiresFailed = 0;
                        if (log.isDebugEnabled())
                            log.debug("batch acquisition of " + (triggers == null ? 0 : triggers.size()) + " triggers");
                    } catch (JobPersistenceException jpe) {
                        if (acquiresFailed == 0) {
                            qs.notifySchedulerListenersError(
                                "An error occurred while scanning for the next triggers to fire.",
                                jpe);
                        }
                        if (acquiresFailed < Integer.MAX_VALUE)
                            acquiresFailed++;
                        continue;
                    } catch (RuntimeException e) {
                        if (acquiresFailed == 0) {
                            getLog().error("quartzSchedulerThreadLoop: RuntimeException "
                                    +e.getMessage(), e);
                        }
                        if (acquiresFailed < Integer.MAX_VALUE)
                            acquiresFailed++;
                        continue;
                    }

                    // 如果获取到待执行的任务列表
                    if (triggers != null && !triggers.isEmpty()) {
                        now = System.currentTimeMillis();
                        // 第一个任务的触发时间（第一个任务一般是最早触发的）
                        long triggerTime = triggers.get(0).getNextFireTime().getTime();
                        // 任务的触发时间与当前的时间差值
                        long timeUntilTrigger = triggerTime - now;
                        // 如果任务触发时间大于当前时间2秒以上
                        while(timeUntilTrigger > 2) {
                            synchronized (sigLock) {
                                if (halted.get()) {
                                    break;
                                }
                                // 查看当前的调度器的状态码和点火时间是否被更改过。如果没有被更改过，则会进入线程等待。
                                // 比如当一个Scheduler添加Job时，我们会将改变该Scheduler的下次点火时间与signaled状态码
                                // org.quartz.core.QuartzScheduler.notifySchedulerThread 里面会调用更改方法
                                // org.quartz.core.QuartzSchedulerThread.signalSchedulingChange 更改状态的代码
                                // 按理应该不可能有这种，但是不排除外层有通过MBean，Remote等方式将新的任务插入进来
                                if (!isCandidateNewTimeEarlierWithinReason(triggerTime, false)) {
                                    try {
                                        // we could have blocked a long while
                                        // on 'synchronize', so we must recompute
                                        // 重新计算并等待真正的触发时间到来
                                        now = System.currentTimeMillis();
                                        timeUntilTrigger = triggerTime - now;
                                        if(timeUntilTrigger >= 1)
                                            sigLock.wait(timeUntilTrigger);
                                    } catch (InterruptedException ignore) {
                                    }
                                }
                            }
                            // wait到期，或者状态码等信息有变更，会调用到这里，上面方法clearSignal=false，这个方法里面clearSignal=true
                            // 应该是为了重置Scheduler状态和时间并且将触发器状态重置，重新进入获取任务列表周期。
                            if(releaseIfScheduleChangedSignificantly(triggers, triggerTime)) {
                                break;
                            }
                            // 重新计算新的等待时间，方便跳出循环
                            now = System.currentTimeMillis();
                            timeUntilTrigger = triggerTime - now;
                        }

                        // this happens if releaseIfScheduleChangedSignificantly decided to release triggers
                        if(triggers.isEmpty())
                            continue;

                        // set triggers to 'executing'
                        List<TriggerFiredResult> bndles = new ArrayList<TriggerFiredResult>();

                        // 如果Scheduler停了，则goAhead=false，并且会被跳出大循环
                        boolean goAhead = true;
                        synchronized(sigLock) {
                            goAhead = !halted.get();
                        }
                        if(goAhead) {
                            try {
                                // 更新触发器的状态并且包装成可执行的TriggerFiredResult对象，等待被执行任务方法。（任务点火，但是没有到真正的执行）
                                List<TriggerFiredResult> res = qsRsrcs.getJobStore().triggersFired(triggers);
                                if(res != null)
                                    bndles = res;
                            } catch (SchedulerException se) {
                                qs.notifySchedulerListenersError(
                                        "An error occurred while firing triggers '"
                                                + triggers + "'", se);
                                //QTZ-179 : a problem occurred interacting with the triggers from the db
                                //we release them and loop again
                                // 释放trigger
                                for (int i = 0; i < triggers.size(); i++) {
                                    qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                }
                                continue;
                            }

                        }

                        // 循环将任务提交到工作线程中
                        for (int i = 0; i < bndles.size(); i++) {
                            TriggerFiredResult result =  bndles.get(i);
                            TriggerFiredBundle bndle =  result.getTriggerFiredBundle();
                            Exception exception = result.getException();
                            // bndle为kong的话，都是exception不为空，肯定是在包装此类的时候发生了异常了。
                            if (exception instanceof RuntimeException) {
                                getLog().error("RuntimeException while firing trigger " + triggers.get(i), exception);
                                qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                continue;
                            }

                            // it's possible to get 'null' if the triggers was paused,
                            // blocked, or other similar occurrences that prevent it being
                            // fired at this time...  or if the scheduler was shutdown (halted)
                            if (bndle == null) {
                                qsRsrcs.getJobStore().releaseAcquiredTrigger(triggers.get(i));
                                continue;
                            }

                            // 创建本地任务的执行脚本（JTAAnnotationAwareJobRunShellFactory）
                            JobRunShell shell = null;
                            try {
                                shell = qsRsrcs.getJobRunShellFactory().createJobRunShell(bndle);
                                // 这个方法会通过newInstance方法来初始化真正的Job实现的类，并存储到内部的 jec = JobExecutionContextImpl 字段中。
                                shell.initialize(qs);
                            } catch (SchedulerException se) {
                                qsRsrcs.getJobStore().triggeredJobComplete(triggers.get(i), bndle.getJobDetail(), CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);
                                continue;
                            }

                            // 将任务执行对象放入到线程池中执行（org.quartz.threadPool.class属性，默认位置为 SimpleThreadPool.class）
                            if (qsRsrcs.getThreadPool().runInThread(shell) == false) {
                                // this case should never happen, as it is indicative of the
                                // scheduler being shutdown or a bug in the thread pool or
                                // a thread pool being used concurrently - which the docs
                                // say not to do...
                                getLog().error("ThreadPool.runInThread() return false!");
                                qsRsrcs.getJobStore().triggeredJobComplete(triggers.get(i), bndle.getJobDetail(), CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);
                            }

                        }

                        continue; // while (!halted)
                    }
                } else { // if(availThreadCount > 0)
                    // should never happen, if threadPool.blockForAvailableThreads() follows contract
                    continue; // while (!halted)
                }

                // 轮回完毕

                long now = System.currentTimeMillis();
                long waitTime = now + getRandomizedIdleWaitTime();
                long timeUntilContinue = waitTime - now;
                synchronized(sigLock) {
                    try {
                      if(!halted.get()) {
                        // QTZ-336 A job might have been completed in the mean time and we might have
                        // missed the scheduled changed signal by not waiting for the notify() yet
                        // Check that before waiting for too long in case this very job needs to be
                        // scheduled very soon
                        if (!isScheduleChanged()) {
                          sigLock.wait(timeUntilContinue);
                        }
                      }
                    } catch (InterruptedException ignore) {
                    }
                }

            } catch(RuntimeException re) {
                getLog().error("Runtime error occurred in main trigger firing loop.", re);
            }
        } // while (!halted)

        // drop references to scheduler stuff to aid garbage collection...
        qs = null;
        qsRsrcs = null;
    }

    private static final long MIN_DELAY = 20;
    private static final long MAX_DELAY = 600000;

    private static long computeDelayForRepeatedErrors(JobStore jobStore, int acquiresFailed) {
        long delay;
        try {
            delay = jobStore.getAcquireRetryDelay(acquiresFailed);
        } catch (Exception ignored) {
            // we're trying to be useful in case of error states, not cause
            // additional errors..
            delay = 100;
        }


        // sanity check per getAcquireRetryDelay specification
        if (delay < MIN_DELAY)
            delay = MIN_DELAY;
        if (delay > MAX_DELAY)
            delay = MAX_DELAY;

        return delay;
    }

    /**
     * 如果当前的调度器的状态码和点火时间被修改过了，则清空任务列表。并返回false跳出循环，重新进入获取任务列表周期。
     * @param triggers    触发器任务列表
     * @param triggerTime 最近的任务触发时间
     * @return boolean true重新开始任务获取周期，false继续往下任务执行
     */
    private boolean releaseIfScheduleChangedSignificantly(
            List<OperableTrigger> triggers, long triggerTime) {
        // 查看当前的调度器的状态码和点火时间是否被更改过。
        if (isCandidateNewTimeEarlierWithinReason(triggerTime, true)) {
            // above call does a clearSignaledSchedulingChange()
            for (OperableTrigger trigger : triggers) {
                // 通知触发器不再计划触发它了
                qsRsrcs.getJobStore().releaseAcquiredTrigger(trigger);
            }
            triggers.clear();
            return true;
        }
        return false;
    }

    /**
     * 判断当前的调度系统是否状态被更改或者点火时间被更改
     * @param oldTime     此时间会与Scheduler点火时间比较（前提是Scheduler点火时间>0），如果大于的话，则相当于认为没有任何改变
     * @param clearSignal 如果判定为更改，则是否在方法执行完毕前清空一下状态码
     * @return boolean false没有改变，true改变了
     */
    private boolean isCandidateNewTimeEarlierWithinReason(long oldTime, boolean clearSignal) {

        // So here's the deal: We know due to being signaled that 'the schedule'
        // has changed.  We may know (if getSignaledNextFireTime() != 0) the
        // new earliest fire time.  We may not (in which case we will assume
        // that the new time is earlier than the trigger we have acquired).
        // In either case, we only want to abandon our acquired trigger and
        // go looking for a new one if "it's worth it".  It's only worth it if
        // the time cost incurred to abandon the trigger and acquire a new one
        // is less than the time until the currently acquired trigger will fire,
        // otherwise we're just "thrashing" the job store (e.g. database).
        //
        // So the question becomes when is it "worth it"?  This will depend on
        // the job store implementation (and of course the particular database
        // or whatever behind it).  Ideally we would depend on the job store
        // implementation to tell us the amount of time in which it "thinks"
        // it can abandon the acquired trigger and acquire a new one.  However
        // we have no current facility for having it tell us that, so we make
        // a somewhat educated but arbitrary guess ;-).

        synchronized(sigLock) {

            if (!isScheduleChanged())
                return false;

            boolean earlier = false;

            // 如果Scheduler点火时间=0或者Scheduler点火时间小于当前等待的任务的点火时间，则任务应该早点出发。
            if(getSignaledNextFireTime() == 0)
                earlier = true;
            else if(getSignaledNextFireTime() < oldTime )
                earlier = true;

            if(earlier) {
                // so the new time is considered earlier, but is it enough earlier?
                // 所以新时间考虑得更早，但早点就够了吗？？
                long diff = oldTime - System.currentTimeMillis();
                // 根据是否进行持久化做的判断，如果做持久化就是70ms,不做则是7ms（其实就是任务快执行了，就不要折腾了）
                if(diff < (qsRsrcs.getJobStore().supportsPersistence() ? 70L : 7L))
                    earlier = false;
            }

            // 是否清理状态，将signaled=false,signaledNextFireTime=0，这样下次循环进来就会返回false
            if(clearSignal) {
                clearSignaledSchedulingChange();
            }

            return earlier;
        }
    }

    public Logger getLog() {
        return log;
    }

} // end of QuartzSchedulerThread
