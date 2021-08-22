package cn.xnatural.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * {@link Step} 任务包装类: 一组顺序相关性的{@link Step}
 *
 * 启动: {@link #run}, 核心方法 {@link #trigger}
 * 任务方法执行顺序: {@link #trigger(Object)}
 * Created by xxb on 18/12/20.
 */
public class TaskWrapper {
    protected static final Logger                  log    = LoggerFactory.getLogger(TaskWrapper.class);
    // 所属TaskContext 执行容器/执行上下文
    protected              TaskContext             ctx;
    /**
     * 当前状态 {@link Status}
     */
    protected final        AtomicReference<Status> status = new AtomicReference<>();
    /**
     * 任务开始时间
     */
    private                Date                    startTime;
    /**
     * 任务唯一标识
     */
    protected Object                               key;
    /**
     * 任务的步骤执行链
     */
    protected final List<Step>                     steps  = new LinkedList<>();
    /**
     * 用于并行步骤执行
     */
    protected ExecutorService executor;


    public TaskWrapper(Object key) { if (key == null) throw new NullPointerException("key must not be null"); this.key = key; }

    public TaskWrapper() { key = "Task[" + Integer.toHexString(hashCode()) + "]"; }


    /**
     * 任务状态
     */
    protected enum Status {Ready, Running, Paused, FailStopped, OkStopped}


    /**
     * 添加执行步骤
     * @param fn 执行逻辑函数
     * @param <I> 入参类型. 入参为上一个{@link Step}的返回
     * @param <R> 输出结果类型. 为下一个{@link Step}的入参
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper step(BiFunction<I, Step, R> fn) { return step(fn, null); }


    /**
     *
     * 添加执行步骤
     * @param fn 执行逻辑函数
     * @param condition 执行条件
     * @param <I> 入参类型. 入参为上一个{@link Step}的返回
     * @param <R> 输出结果类型. 为下一个{@link Step}的入参
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper step(BiFunction<I, Step, R> fn, Predicate<Step> condition) {
        if (fn == null) throw new IllegalArgumentException("Param fn required");
        steps.add(new Step<>(this, fn, condition));
        return this;
    }


    /**
     * 可重复执行的步骤
     * @param limit 执行的次数限制
     * @param fn 执行逻辑函数
     * @param isReRun 判断是否需要重新执行. true: 重试 参数1: fn执行的结果, 参数2: 当前第几次重试
     * @param condition 执行条件
     * @param <I> 入参类型
     * @param <R> 输出结果类型
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper reStep(int limit, BiFunction<I, Step, R> fn, BiFunction<R, Step, Boolean> isReRun, Predicate<Step> condition) {
        if (limit < 1) throw new IllegalArgumentException("Param limit must > 0");
        steps.add(new Step<I, R>(this, fn, condition) {
            @Override
            protected boolean needReRun(R r) {
                if (times() > limit) return false;
                return isReRun.apply(r, this);
            }
        });
        return this;
    }


    /**
     * 可重复执行的步骤
     * @param limit 执行的次数限制
     * @param fn 执行逻辑函数
     * @param isReRun 判断是否需要重新执行. true: 重试 参数1: fn执行的结果, 参数2: 当前第几次重试
     * @param <I> 入参类型
     * @param <R> 输出结果类型
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper reStep(int limit, BiFunction<I, Step, R> fn, BiFunction<R, Step, Boolean> isReRun) {
        return reStep(limit, fn, isReRun, null);
    }


    /**
     * 并行多个步骤
     * @param condition 执行条件
     * @param steps 步骤函数
     * @param <I> 入参类型
     * @param <R> 输出结果类型
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper parallel(Predicate<Step> condition, BiFunction<I, Step, R>... steps) {
        this.steps.add(
                new Step<I, List<R>>(this, (i, me) -> {
                    final CountDownLatch latch = new CountDownLatch(steps.length);
                    final List<R> results = new ArrayList<>(steps.length);
                    for (int j = 0; j < steps.length; j++) {
                        BiFunction<I, Step, R> step = steps[j];
                        int finalJ = j;
                        results.add(null);
                        exec(() -> {
                            results.set(finalJ, step.apply(i, me)); // 返回的结果list 和 入参一一对应
                            latch.countDown();
                        });
                    }
                    try { latch.await(); } catch (InterruptedException e) { throw new RuntimeException(e); }
                    return results;
                }, condition)
        );
        return this;
    }


    /**
     * 并行多个步骤
     * @param steps 步骤函数
     * @param <I> 入参类型
     * @param <R> 输出结果类型
     * @return {@link TaskWrapper}
     */
    public <I, R> TaskWrapper parallel(BiFunction<I, Step, R>... steps) { return parallel(null, steps); }

    /**
     * 执行任务
     * @return 任务结果
     */
    public final Object run() { return run(null); }


    /**
     * 执行任务
     * @param input 输入
     * @return 任务结果
     */
    public final Object run(Object input) {
        if (Status.OkStopped == status.get() || Status.FailStopped == status.get()) {
            log.warn(logPrefix() + "already closed"); return null;
        }
        if (Status.Running == status.get()) {
            log.warn(logPrefix() + "already running"); return null;
        }
        if (Status.Paused == status.get()) {
            log.warn(logPrefix() + "already paused"); return null;
        }
        status.compareAndSet(null, Status.Ready);
        this.startTime = new Date();
        log.debug(logPrefix() + "starting");
        if (steps.isEmpty()) log.warn(logPrefix() + "not found steps");
        return trigger(input);
    }


    /**
     * 触发任务步骤链执行
     * 自旋执行
     * @param input 入参
     */
    protected final Object trigger(Object input) {
        if (!status.compareAndSet(Status.Ready, Status.Running)) return null; // 保证同时只有一个线程执行任务
        Object result = input;
        for (Step step : steps) {
            if (Status.Paused == status.get()) break; // 暂停
            if (step.isCompleted()) { result = step.getResult(); continue; }
            if (step.condition != null) {
                synchronized (this) {
                    if (!step.condition.test(step)) { // 不满足执行条件, 暂停等待恢复执行
                        status.set(Status.Paused); break;
                    }
                }
            }
            try {
                while (true) { // 循环执行直到成功
                    Object r = step.apply(result);
                    if (step.isCompleted()) {result = r; break;}
                    if (Status.Paused == status.get()) break;
                }
            } catch (Exception ex) {
                log.error(logPrefix() + "Step error", ex);
                status.set(Status.FailStopped);
            }
        }
        // 全部完成则结束任务
        if (Status.FailStopped != status.get() && steps.stream().filter(Step::isCompleted).count() == steps.size()) status.set(Status.OkStopped);
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) {
            log.info(logPrefix() + "finished. spend: {}ms. status: {}",  System.currentTimeMillis() - startTime.getTime(), status.get());
            if (ctx != null) ctx.removeTask(this);
        }
        return result;
    }


    /**
     * 暂停. 任务会执行完当前正在执行的步骤后暂停执行下一个{@link Step}
     */
    public boolean suspend() {
        if (status.get() == Status.Paused) return true;
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        return status.compareAndSet(Status.Running, Status.Paused);
    }


    /**
     * 恢复执行
     */
    public synchronized boolean resume() {
        if (status.get() == Status.Running) return true;
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) return false;
        if (status.compareAndSet(Status.Paused, Status.Ready)) {
            exec(() -> trigger(null)); return true;
        }
        return false;
    }


    /**
     * 设置任务独立执行线程池
     * 一般用于任务独立运行 并且 有 并行步骤时
     * @param executor 线程池
     * @return {@link TaskWrapper}
     */
    public TaskWrapper executor(ExecutorService executor) { this.executor = executor; return this; }


    /**
     * 任务执行步骤函数
     * @param fn 执行函数
     */
    protected void exec(Runnable fn) {
        if (ctx() != null) {
            ctx().exec(fn);
        }
        else {
            final Runnable fnn = () -> {
                try { fn.run(); } catch (Exception ex) {
                    log.error("", ex);
                }
            };
            if (executor == null || executor.isShutdown()) fnn.run();
            else executor.execute(fnn);
        }
    }


    /**
     * 日志封装
     * @return {@link Logger}
     */
    public Logger log() { return TaskWrapper.log; }


    /**
     * 当前任务所在容器
     */
    public TaskContext ctx() { return ctx; }


    /**
     * 是否成功结束
     */
    boolean isSuccessEnd() { return status.get() == Status.OkStopped; }


    /**
     * 任务状态
     */
    public String getStatus() { return status.get().name(); }


    /**
     * 启动时间
     */
    public Date getStartTime() { return startTime; }


    /**
     * 任务标识key
     */
    public Object getKey() { return key; }


    /**
     * 日志前缀
     */
    protected String logPrefix() { return (ctx() == null ? "" : ctx().key + ", ") + getKey() + " -> ";}


    @Override
    public String toString() {
        return logPrefix() + "[startTime: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(startTime) + ", stepTotal: " + steps.size() + ", completed: " + steps.stream().filter(step -> step.isCompleted()).count() + ", status: " + getStatus() + "]";
    }
}
