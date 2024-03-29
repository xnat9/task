package cn.xnatural.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 执行上下文/任务调度/执行容器: 一组逻辑相关性的{@link TaskWrapper}
 * 1. 并行任务
 * 2. 衍生任务
 * 3. 任务调度
 *      任务流程: 添加{@link #addTask}, 启动{@link #trigger()}, 结束{@link #removeTask}
 * 适合: 网络爬虫
 * @param <T> Task 类型
 */
public class TaskContext<T extends TaskWrapper> {
    protected static final Logger            log            = LoggerFactory.getLogger(TaskContext.class);
    /**
     * 所有任务通过 executor 执行
     */
    protected              ExecutorService   executor;
    /**
     * Context 唯一标识
     */
    protected String                         key;
    /**
     * 等待执行的任务对列
     */
    protected final        Queue<T>          waitingTasks   = new ConcurrentLinkedQueue<>();
    /**
     * 正在执行的任务对列
     */
    protected final        Queue<T>          executingTasks = new ConcurrentLinkedQueue<>();
    /**
     * 容器能运行的Task最大个数限制, 即: 并行Task的个数限制
     * {@link #executingTasks}
     */
    protected int                            parallelLimit  = 10;
    /**
     * 失败了多少个task
     */
    protected final LongAdder                failureCnt     = new LongAdder();
    /**
     * 成功了多少个task
     */
    protected final LongAdder                successCnt     = new LongAdder();
    /**
     * 开始时间
     */
    protected      Date                      startTime;
    /**
     * 当前状态
     */
    protected final  AtomicReference<Status> status         = new AtomicReference<>();
    /**
     * 属性集
     */
    protected final Map<String, Object>      attrs          = new ConcurrentHashMap<>(7);


    /**
     * 创建集任务管理
     * @param key 集任务key
     * @param executor 执行器
     */
    public TaskContext(String key, ExecutorService executor) {
        if (key == null || key.isEmpty()) {
            this.key = "TaskContext[" + Integer.toHexString(hashCode()) + "]";
        }
        this.executor = executor == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            final AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) { return new Thread(r, TaskContext.this.key + "-" + i.getAndIncrement()); }
        }) : executor;
    }
    public TaskContext(String key) { this(key, null); }
    public TaskContext() { this(null, null); }


    /**
     * 容器状态
     */
    protected enum Status {Ready, Running, Paused, Stopping, FailStopped, OkStopped}


    /**
     * TaskContext 容器启动
     */
    public final TaskContext<T> start() {
        if (status.get() != null) {
            log.warn(key + " -> already " + status.get()); return this;
        }
        if (!status.compareAndSet(null, Status.Ready)) return this; // 保证只一个线程执行过start
        startTime = new Date();
        log.info(key + " -> starting");
        try {
            doStart(this);
            if (waitingTasks.isEmpty()) {
                log.warn(key + " -> not found task");
                status.set(Status.OkStopped);
                doStop(this); return this;
            }
            trigger();
        } catch (Exception t) {
            log.error(key + " -> start fail", t);
            status.set(Status.FailStopped);
            doStop(this);
        }
        return this;
    }


    /**
     * 启动前初始化
     */
    protected void doStart(TaskContext<T> ctx) { }


    /**
     * 触发任务执行. 自旋执行
     */
    protected final void trigger() {
        // 触发任务执行. 1. 当前状态为Running; 2. 当前状态为Ready
        if (status.get() == Status.Running || status.compareAndSet(Status.Ready, Status.Running)) {
            while (!waitingTasks.isEmpty() && status.get() == Status.Running && executingTasks.size() < parallelLimit) {
                T task = waitingTasks.poll();
                if (task == null) break;
                executingTasks.add(task);
                exec(task::run); // 每个Task开始, 用一个新的执行栈
            }
        }
        // 暂停所有正在执行的任务
        if (status.get() == Status.Paused) {
            for (T t : executingTasks) t.suspend();
        }
        // 主动停止时, 执行队列全都是暂停任务, 则尝试恢复所有执行
        if (status.get() == Status.Stopping && executingTasks.stream().allMatch(t -> t.status.get() == TaskWrapper.Status.Paused)) {
            for (T t : executingTasks) t.resume();
        }
        // 判断是否已结束
        if (
                status.get() != Status.Paused && executingTasks.isEmpty() &&
                (
                    (status.get() == Status.Running && waitingTasks.isEmpty() && status.compareAndSet(Status.Running, failureCnt.longValue() > 0 ? Status.FailStopped : Status.OkStopped)) ||
                    (status.get() == Status.Stopping && status.compareAndSet(Status.Stopping, failureCnt.longValue() > 0 ? Status.FailStopped : Status.OkStopped))
                )
        ) {
            doStop(this);
        }
    }


    /**
     * 结束执行
     */
    protected void doStop(TaskContext<T> ctx) {
        log.info(key + " -> finished({}). spend: {}ms, successCnt: {}, failureCnt: {}, waiting: {}", status.get(), System.currentTimeMillis() - startTime.getTime(), successCnt, failureCnt, waitingTasks.size());
        executor.shutdown();
    }


    /**
     * 添加任务前执行
     * @param task {@link TaskWrapper}
     * @return true: 添加, false: 不添加
     */
    protected boolean preAddTask(final T task) { return true; }


    /**
     * 添加任务
     * 进入等待执行对列
     */
    public final TaskContext<T> addTask(final T task) {
        if (status.get() == Status.OkStopped || status.get() == Status.FailStopped) throw new RuntimeException(key + " already stopped. Cannot add task: " + task.key);
        if (task == null) {
            log.warn(key + " -> add task is null"); return this;
        }
        if (!preAddTask(task)) return this;
        task.ctx = this;
        waitingTasks.offer(task);
        log.debug("{} -> added task: {}", key, task.key);
        trigger();
        return this;
    }


    /**
     * 删除一个Task 之前 做的操作
     * @param task {@link TaskWrapper}
     */
    protected void preRemoveTask(T task) {}


    /**
     * 从正在执行对列中 移除
     * @param task {@link TaskWrapper}
     */
    protected final void removeTask(final T task) {
        log.trace(key + " -> remove task: {}", task.key);
        preRemoveTask(task);
        if (task.isSuccessEnd()) successCnt.increment();
        else failureCnt.increment();
        executingTasks.remove(task); // 从执行对列中移除Task
        postRemoveTask(task);
        trigger();
    }


    /**
     * 删除一个Task 之后 做的操作
     */
    protected void postRemoveTask(T task) {}


    /**
     * 主动关闭执行. 正在运行的任务全部停止后,再停止容器
     */
    public boolean stop() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        if (status.get() == Status.Stopping) {
            trigger();
            log.info(key + " -> stopping({}). spend: {}ms, successCnt: {}, failureCnt: {}, waiting: {}, executing: {}", status.get(), System.currentTimeMillis() - startTime.getTime(), successCnt, failureCnt, waitingTasks.size(), executingTasks);
            return true;
        }
        boolean f = status.compareAndSet(Status.Running, Status.Stopping) || status.compareAndSet(Status.Paused, Status.Stopping);
        log.info(key + " -> stopping: {}, status:{}", f, status.get());
        trigger();
        return f;
    }


    /**
     * 暂停
     */
    public boolean suspend() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        if (status.get() == Status.Paused) return true;
        boolean f = status.compareAndSet(Status.Running, Status.Paused);
        log.info(key + " -> suspend: {}, status:{}", f, status.get());
        trigger();
        return f;
    }


    /**
     * 恢复,继续执行
     */
    public boolean resume() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        if (status.get() == Status.Running) return true;
        boolean f = status.compareAndSet(Status.Paused, Status.Ready);
        log.info(key + " -> resume: {}, status:{}", f, status.get());
        trigger();
        return f;
    }


    /**
     * 恢复某一个任务
     * @param key 任务标识key
     */
    public void resumeTask(String key) {
        for (T task : executingTasks) {
            if (Objects.equals(task.key, key)) {
                exec(task::resume); break;
            }
        }
    }


    /**
     * 向当前容器中的 线程池 中 添加任务
     */
    public final void exec(final Runnable fn) {
        final Runnable fnn = () -> {
            try { fn.run(); } catch (Exception ex) {
                log.error("", ex);
            }
        };
        if (executor == null || executor.isShutdown()) fnn.run();
        else executor.execute(fnn);
    }


    /**
     * 任务是否全部成功结束
     */
    public boolean isSuccessEnd() { return isComplete() && failureCnt.longValue() == 0; }


    /**
     * 任务是否全部完成
     */
    public boolean isComplete() { return isEnd() && waitingTasks.isEmpty() && executingTasks.isEmpty(); }


    /**
     * 容器是否结束
     */
    public boolean isEnd() { return Status.OkStopped == status.get() || Status.FailStopped == status.get(); }


    /**
     * 设置属性
     * @param key 属性key
     * @param value 属性值
     * @return {@link TaskContext<T>}
     */
    public TaskContext<T> setAttr(String key, Object value) {
        this.attrs.put(key, value);
        return this;
    }


    /**
     * 获取属性
     * @param key 属性key
     * @return 属性值
     */
    public Object getAttr(String key) { return attrs.get(key); }


    /**
     * 设置并发任务大小. 默认10个
     */
    public TaskContext<T> setParallelLimit(int parallelLimit) {
        if (parallelLimit < 1) throw new IllegalArgumentException("Param parallelLimit >= 1");
        this.parallelLimit = parallelLimit;
        return this;
    }


    @Override
    public String toString() {
        return key + " -> [successCnt: " + successCnt + ", failureCnt: " + failureCnt + " , spend: " + (System.currentTimeMillis() - startTime.getTime()) + "ms, waiting: " + waitingTasks.size() + ", executing: " + executingTasks.size() + "]";
    }
}
