package cn.xnatural.task;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LocationAwareLogger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * 任务执行单元
 * @param <I> 入参. 一般为上一个{@link Step}的执行结果{@link #result}
 * @param <R> 结果. 一般为下一个{@link Step}的入参{@link I}
 */
public class Step<I, R> {
    // 执行单元
    private final BiFunction<I, Step, R> fn;
    // 执行条件
    protected final Predicate<Step> condition;
    // 当前关联的任务
    private final TaskWrapper task;
    // 是否正在执行
    private final AtomicBoolean running = new AtomicBoolean(false);
    // 执行结果
    private R result;
    // 是否执行结束
    protected boolean end;
    // 执行第几次
    private int times;
    // 第几个步骤
    public final int num;


    public Step(TaskWrapper task, BiFunction<I, Step, R> fn, Predicate<Step> condition) {
        this.task = task; this.fn = fn; this.condition = condition;
        this.num = task == null ? -1 : task.steps.size() + 1;
    }


    /**
     * 是否需要重试
     * @param r 执行结果
     */
    protected boolean needReRun(R r) { return false; }


    /**
     * 是否已完成
     */
    public boolean isCompleted() { return end; }


    /**
     * 执行步骤
     * @param input 入参
     * @return 结果
     */
    protected R apply(I input) {
        if (end) return result;
        if (running.compareAndSet(false, true)) {
            times++;
            result = fn.apply(input, this);
            if (needReRun(result)) result = null;
            else end = true;
            running.set(false);
        }
        return result;
    }


    /**
     * 执行的结果
     */
    public R getResult() { return result; }


    /**
     * 执行第几次
     */
    public int times() { return times; }


    /**
     * 当前关联的任务
     */
    public TaskWrapper task() { return task; }


    /**
     * 当前关联的任务容器
     */
    public TaskContext ctx() { return task == null ? null : task.ctx(); }


    /**
     * 日志
     */
    public Logger log() { return TaskWrapper.log; }


    protected void doLog(Level level, String msg, Object[] args, Throwable ex) {
        ((LocationAwareLogger) TaskWrapper.log).log(null, Step.class.getName(), level.toInt(), task().logPrefix() + msg, args, ex);
    }


    public void info(String msg, Object...args) { doLog(Level.INFO, msg, args, null); }


    public void debug(String msg, Object...args) {
        if (TaskWrapper.log.isDebugEnabled()) {
            doLog(Level.DEBUG, msg, args, null);
        }
    }


    public void trace(String msg, Object...args) {
        if (TaskWrapper.log.isTraceEnabled()) {
            doLog(Level.TRACE, msg, args, null);
        }
    }


    public void warn(String msg, Object...args) { doLog(Level.WARN, msg, args, null); }


    public void error(Throwable ex, String msg, Object...args) { doLog(Level.ERROR, msg, args, ex); }
}
