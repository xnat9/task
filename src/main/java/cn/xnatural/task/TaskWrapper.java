package cn.xnatural.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * {@link Step} 任务包装类: 一组顺序相关性的{@link Step}
 *
 * 启动: {@link #start}, 核心方法 {@link #trigger}
 * 任务方法执行顺序: {@link #trigger()}
 * Created by xxb on 18/1/10.
 */
public class TaskWrapper {
    protected static final Logger                log         = LoggerFactory.getLogger(TaskWrapper.class);
    // 所属TaskContext 执行容器/执行上下文
    protected              TaskContext           ctx;
    /**
     * 当前状态 {@link Status}
     */
    protected final        AtomicReference<Status>       status      = new AtomicReference<>();
    /**
     * 是否应该关闭
     */
    private final          AtomicBoolean         shouldStop  = new AtomicBoolean(false);
    /**
     * 保存当前正在执行的Step
     */
    private                AtomicReference<Step> currentStep = new AtomicReference<>();
    /**
     * 任务开始时间
     */
    private                Date                  startupTime;
    /**
     * 任务唯一标识
     */
    protected Object                key;

    /**
     * Task 超时时间, 单位毫秒
     * 默认10分钟.
     * TODO 检测超时的Task
     */
    protected              long                  timeout     = 1000 * 60 * 10;
    protected final List<Step>  steps = new LinkedList<>();


    public TaskWrapper(Object key) { this.key = key; }

    public TaskWrapper() {
        key = "Task[" + Integer.toHexString(hashCode()) + "]";
    }


    /**
     * 任务状态
     */
    protected enum Status {Ready, Running, Paused, FailStopped, OkStopped}


    /**
     * 添加执行步骤
     * @param fn 执行逻辑函数
     * @param <I> 入参类型. 入参为上一个{@link Step}的返回
     * @param <R> 输出结果类型. 为下一个{@link Step}的入参
     * @return
     */
    public <I, R> TaskWrapper step(BiFunction<I, Step, R> fn) {
        steps.add(new Step<>(this, fn));
        return this;
    }


    /**
     * 可重复执行的步骤
     * @param limit 执行的次数限制
     * @param fn 执行逻辑函数
     * @param isReRun 判断是否需要重新执行. true: 重试 参数1: fn执行的结果, 参数2: 当前第几次重试
     * @param <I> 入参类型
     * @param <R> 输出结果类型
     * @return
     */
    public <I, R> TaskWrapper reStep(int limit, BiFunction<I, Step, R> fn, BiFunction<R, Step, Boolean> isReRun) {
        if (limit < 1) throw new IllegalArgumentException("limit must > 0");
        steps.add(new Step<I, R>(this, fn) {
            @Override
            protected boolean needReRun(R r) {
                if (times() > limit) throw new RuntimeException("Re run up to limit: " + limit);
                return isReRun.apply(r, this);
            }
        });
        return this;
    }


    /**
     * Task 启动
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
        this.startupTime = new Date();
        log.debug(key + " -> starting");
        if (steps.isEmpty()) log.warn(getKey() + " -> not found steps");
        trigger();
    }


    /**
     * 触发任务步骤链执行
     * 自旋执行
     */
    protected final void trigger() {
        // if (Status.OkStopped == status.get() || Status.FailStopped == status.get()) return;
        if (!status.compareAndSet(Status.Ready, Status.Running)) return; // 保证同时只有一个线程执行任务
        Object result = null;
        for (Step step : steps) {
            if (Status.Paused == status.get()) break; // 暂停
            if (step.isCompleted()) { result = step.getResult(); continue; }
            try {
                while (true) { // 循环执行直到成功
                    Object r = step.apply(result, this);
                    if (step.isCompleted()) {result = r; break;}
                    if (Status.Paused == status.get()) break;
                }
            } catch (Exception ex) {
                log.error("Step error", ex);
                status.set(Status.FailStopped);
            }
        }
        // 全部完成则结束任务
        if (steps.stream().filter(Step::isCompleted).count() == steps.size()) status.set(Status.OkStopped);
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) {
            log.info(key + " -> finished. spend: {}ms. status: {}",  System.currentTimeMillis() - startupTime.getTime(), status.get());
            if (ctx != null) ctx.removeTask(this);
        }
    }


    /**
     * 暂停. 任务会执行完当前正在执行的步骤后暂停执行下一个{@link Step}
     * @return
     */
    public boolean suspend() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) { return false; }
        if (status.get() == Status.Paused) return true;
        // 正在执行最后一个Step 不能暂停
        if (status.get() == Status.Running && (steps.stream().filter(step -> step.isCompleted()).count() + 1) == steps.size()) return false;
        return status.compareAndSet(Status.Running, Status.Paused);
    }


    /**
     * 恢复执行
     * @return
     */
    public boolean resume() {
        if (Status.FailStopped == status.get() || Status.OkStopped == status.get()) { return false; }
        if (status.get() == Status.Running) return true;
        status.set(Status.Ready);
        trigger();
        return true;
    }


    public Logger log() {return TaskWrapper.log;}


    /**
     * 当前任务所在容器
     * @return
     */
    public TaskContext ctx() {return ctx;}


    /**
     * 是否成功结束
     * @return
     */
    boolean isSuccessEnd() { return status.get() == Status.OkStopped; }


    /**
     * 任务状态
     * @return
     */
    public String getStatus() { return status.get().name(); }


    /**
     * 启动时间
     * @return
     */
    public Date getStartupTime() { return startupTime; }


    /**
     * 任务标识key
     * @return
     */
    public Object getKey() { return key; }


    @Override
    public String toString() {
        return key + " -> [startTime: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startupTime) + ", stepTotal: " + steps.size() + ", completed: " + steps.stream().filter(step -> step.isCompleted()).count() + ", status: " + getStatus() + "]";
    }
}
