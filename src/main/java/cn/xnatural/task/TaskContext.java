package cn.xnatural.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * 任务{@link T}执行的上下文/管理者/执行容器: 一组逻辑相关性的{@link TaskWrapper}
 * 提供任务{@link T}的管理及调度功能
 * 启动容器: {@link #start}, 停止容器: {@link #stop}
 * 启动前初始化 {@link #doStart}, 停止时执行 {@link #doStop}
 * 触发容器继续执行 {@link #trigger}
 * 管理多个任务的并行执行.包括任务的添加{@link #addTask}, 启动{@link #startTask}, 结束{@link #removeTask}
 * @param <T> Task 类型
 */
public class TaskContext<T extends TaskWrapper> {
    protected static final Logger   log            = LoggerFactory.getLogger(TaskContext.class);
    /**
     * 所有任务通过 executor 执行
     */
    protected              Executor executor;
    /**
     * Context 唯一标识
     */
    protected String   key;
    /**
     * 等待执行的任务对列
     */
    protected final        Queue<T> waitingTasks   = new ConcurrentLinkedQueue<>();
    /**
     * 正在执行的任务对列
     */
    protected final        Queue<T> executingTasks = new ConcurrentLinkedQueue<>();
    /**
     * 容器能运行的Task最大个数限制, 即: 并行Task的个数限制
     * {@link #executingTasks}
     */
    protected int                           parallelLimit  = 10;
    /**
     * 失败了多少个task
     */
    protected final LongAdder                           failureCnt = new LongAdder();
    /**
     * 成功了多少个task
     */
    protected final LongAdder                           successCnt = new LongAdder();
    /**
     * 开始时间
     */
    protected      Date                                startTime;
    /**
     * 结束时间
     */
    protected       Date                                endTime;
    /**
     * 状态
     */
    protected final  AtomicReference<Status> status     = new AtomicReference<>();


    public TaskContext(String key) {
        if (key == null || !key.isEmpty()) throw new IllegalArgumentException("key is must not empty");
        this.key = key;
    }
    public TaskContext() { this.key = "TaskContext[" + Integer.toHexString(hashCode()) + "]"; }


    /**
     * 容器状态
     */
    protected enum Status {Ready, Running, Paused, Stopping, FailStopped, OkStopped}


    /**
     * TaskContext 容器启动
     */
    public final void start() {
        if (Status.OkStopped == status.get() || Status.FailStopped == status.get()) {
            log.warn(key + " -> already closed"); return;
        }
        if (Status.Running == status.get()) {
            log.warn(key + " -> already running"); return;
        }
        if (Status.Paused == status.get()) {
            log.warn(key + " -> already paused"); return;
        }
        status.compareAndSet(null, Status.Ready);
        this.startTime = new Date();
        log.info(key + " -> starting");
        try {
            doStart(this);
            if (waitingTasks.isEmpty()) {
                log.warn(key + " -> not found task");
                endTime = new Date(); status.set(Status.FailStopped);
                return;
            }
            trigger();
        } catch (Exception t) {
            log.error(key + " -> start fail", t);
            endTime = new Date(); status.set(Status.FailStopped);
        }
    }


    /**
     * 主动关闭执行. 正在运行的任务全部停止后,再停止容器
     */
    public boolean stop() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        if (status.get() == Status.Stopping) return true;
        boolean f = status.compareAndSet(Status.Running, Status.Stopping) || status.compareAndSet(Status.Paused, Status.Stopping);
        trigger();
        return f;
    }


    /**
     * 触发任务执行
     * 自旋执行
     */
    protected final void trigger() {
        if (status.get() == Status.Running || !status.compareAndSet(Status.Ready, Status.Running)) return;
        while (!waitingTasks.isEmpty() && status.get() == Status.Running && executingTasks.size() < parallelLimit) {
            T task = waitingTasks.poll();
            startTask(task);
        }
    }

//    /**
//     * TaskContext 容器关闭
//     */
//    final void stop() {
//        // running 已经为false了则忽略, 因为可能被同时多线程执行(同时执行checkShouldStop)
//        if (!running.compareAndSet(true, false)) return
//                log.trace(logPrefix + "停止")
//
//        this.endTime = new Date()
//        try {
//            doStop()
//        } catch (Exception e) {
//            log.error(logPrefix + "stop回调错误", e)
//        }
//
//        log.info(logPrefix + "结束. 成功了 {} 个Task, 失败了 {} 个Task, 共执行 " +(getEndTime().getTime() - getStartTime().getTime())+ " 毫秒",
//                successCnt,
//                failureCnt,
//                )
//
//        // 依次关闭所有资源
//        try {
//            if (!sharedExecutor && executor instanceof ExecutorService) {
//                log.trace(logPrefix + "关闭线程池: {}", executor)
//                ((ExecutorService) executor).shutdown()
//            }
//        } catch (Exception e) {
//            log.error(logPrefix + "线程池关闭错误", e)
//        }
//    }


    protected void doStop(TaskContext ctx) {}


    /**
     * 暂停
     */
    public boolean suspend() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) {
            throw new RuntimeException(key + " already stopped");
        }
        if (status.get() == Status.Paused) return true;
        // 正在执行最后一个Step 不能暂停
        if (status.get() == Status.Running && waitingTasks.isEmpty() && executingTasks.size() == 1) return false;
        return status.compareAndSet(Status.Running, Status.Paused);
    }


    /**
     * 恢复
     */
    TaskContext<T> resume() {this.pause = false; this}


    /**
     * 向当前容器中的 线程池 中 添加任务
     *
     * @param fn
     */
    protected final void exec(Runnable fn) {
        if (executor == null) fn.run();
        else executor.execute(fn);
    }


    /**
     * 启动一个 Task, Task开始走自己的生命周期
     * 如果 此方法为 private 就不必做验证了. 具体验证请看 {@link #trigger}
     * @param task
     */
    protected void startTask(T task) {
        if (task == null) return;
        executingTasks.add(task);
        // 每个Task开始, 用一个新的执行栈
        exec(task::start);
    }


//    /**
//     * 触发容器继续执行
//     * 尝试从排对对对列中 取出Task 并启动
//     * 慢启动特性 {@link #oneTimeTaskLimit} 即: 不让容器正在行动的Task个数一下子到达最大值{@link #parallelLimit}
//     *
//     * @return 如果没有Task启动, 返回 false.
//     */
//    protected final boolean trigger() {
//        boolean ret = false
//        // 可以启动新Task的条件: 1.容器处于运行状态, 2.没有被通知stop, 3.没有处于暂停状态, 4.没有逻辑阻止启动新Task, 5.等待执行对列不为空, 6.执行对列没有达到最大个数
//        final Predicate p = (ctx) -> (running.get() && !stopSingle && !pause && !waitingTasks.isEmpty() && executingTasks.size() < parallelLimit)
//        if (oneTimeTaskLimit > 0) {
//            // TODO 如果有多个线程同时执行到这里, 慢启动特性就失效了
//            int limit = oneTimeTaskLimit
//            while (p.test(this) && limit > 0) {
//                limit--
//                def t = waitingTasks.poll()
//                if (t) {
//                    ret = true
//                    startTask(t)
//                }
//            }
//        } else {
//            while (p.test(this)) {
//                def t = waitingTasks.poll()
//                if (t) {
//                    ret = true
//                    startTask(t)
//                }
//            }
//        }
//        // 当没有可启动的Task时, 检查是否有超时Task存在
////        if (!ret) {
////            // TODO: 有可能只有当等待对列执行完了, 才会执行到这里来
////            if (!executingTasks.isEmpty()) exec(() -> {
////                executingTasks.forEach(t -> {
////                    // NOTE: 多线程环境下, 有可能这个t 还没有执行start(), 也即是还没有开始Task
////                    if (t.getStartupTime() != null && t.isTimeout()) log.warn("检测到超时Task: {}", t.getKey())
////                })
////            })
////        }
//        return ret
//    }


    /**
     * 删除一个Task 之前 做的操作
     *
     * @param task
     */
    protected void preRemoveTask(T task) {}


    /**
     * 删除一个Task, 一般是 在Task 结束后, 会调用此方法
     *
     * @param task
     */
    protected final void removeTask(final T task) {
        log.trace(logPrefix + "移除Task: {}", task)
        preRemoveTask(task)
        if (task.isSuccessEnd()) successCnt.increment()
        else failureCnt.increment()
        executingTasks.remove(task) // 从执行对列中移除Task
        postRemoveTask(task)

        // 尝试启动新的Task, 如果没有可启动的Task, 则检查是否应该停止容器
        if (!trigger()) tryStop()
    }


    /**
     * 删除一个Task 之后 做的操作
     *
     * @param task
     */
    protected void postRemoveTask(T task) {}


    /**
     * 添加任务
     * 进入等待执行对列
     *
     * @param task
     */
    final TaskContext<T> addTask(final T task) {
        if (endTime != null) throw new RuntimeException("$key already stopped. Cannot add task: $task.key")
        if (task == null) {
            log.warn(logPrefix + "添加Task为空, 忽略!")
            return this
        }
        waitingTasks.offer(task)
        task.setCtx(this)
        trigger()
        return this
    }


    /**
     * 启动前初始化
     */
    protected void doStart(TaskContext ctx) { }


    /**
     * 检查是否应该结束当前 TaskContext 容器
     * @return true 可以停止, false 不能停止
     */
    protected boolean tryStop() {
        // 容器结束条件: 没有正在执行的任务, 不是暂停状态, 处于运行状态.
        if (executingTasks.isEmpty() && !pause && running.get()) {
            exec(() -> stop())
            return true
        }
        return false;
    }


    /**
     * @param executor
     * @param shared   是否为共享线程池
     * @return
     */
    final TaskContext<T> setExecutor(Executor executor, boolean shared = true) {
        if (running.get()) throw new UnsupportedOperationException("运行状态不能设置executor")
        Objects.requireNonNull(executor, "参数 executor 不能为空")
        this.executor = executor
        sharedExecutor = shared
        return this
    }


    @Override
    boolean equals(Object obj) {
        if (this == obj) return true
        else if (obj instanceof TaskContext) {
            return Objects.equals(this.getKey(), ((TaskContext) obj).getKey())
        }
        return false
    }


    @Override
    String toString() {
        return Objects.toString(getKey(), "") +
                ": 成功了 " + successCnt + " 个Task, 失败了 " + failureCnt + " 个Task, 已执行 " +
                (System.currentTimeMillis() - getStartTime().getTime()) + " 毫秒, 正在排对 " + waitingTasks.size() + " 个, 正在执行的Task: " + executingTasks
    }


    /**
     * 结束时对当前 TaskContest 做一个 summary 信息摘要
     * @return
     */
    String lastSummary() {
        if (endTime == null) return null
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return Objects.toString(key, "") +
                ": 开始于 " + sdf.format(getStartTime()) + ", 是否是手动停止: " + stopSingle + ", 成功了 " + successCnt + " 个Task, 失败了 " + failureCnt + " 个Task, 共执行 " +
                (getEndTime().getTime() - getStartTime().getTime()) + " 毫秒"
    }
}
