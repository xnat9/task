import cn.xnatural.task.TaskContext;
import cn.xnatural.task.TaskWrapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTest {

    final static Logger log = LoggerFactory.getLogger("ROOT");

    @Test
    void taskTest() throws Exception {
        AtomicInteger var = new AtomicInteger();
        TaskWrapper task = new TaskWrapper()
                // 添加执行步骤
                .step((input, step) -> {
                    step.info("执行 step{} ... ", step.num);
                    return "xxx";
                })
                .executor(Executors.newFixedThreadPool(2))
                // 当前步骤并行执行多个函数
                .parallel(
                        (input, step) -> {
                            step.info("parallel 1");
                            return "parallel 1";
                        },
                        (input, step) -> {
                            step.info("parallel 2");
                            return "parallel 2";
                        }
                )
                // 条件步骤
                .step((param, step) -> {
                    step.info("执行 step{} ... ", step.num);
                    return "xxx";
                }, step -> var.get() > 0) // 当前执行条件判断函数
                // 这是个重试执行步骤(最多执行5次)
                .reStep(5, (input, step) -> {
                    step.info("执行重试step{}, 第 {} 次", step.num, step.times());
                    return new Random().nextInt(10) % 2 == 0 ? null : input + " end";
                }, (result, step) -> { // 判断是否重试函数
                    if (result == null) return true;
                    else return false;
                });
        task.run();
        Thread.sleep(1000L * 5);
        var.incrementAndGet();
        task.resume(); // 恢复执行
        Thread.sleep(1000L * 5);
    }


    @Test
    void emptyStepTask() { new TaskWrapper("empty").run(); }


    @Test
    void testTaskParallel() {
        Object r = new TaskWrapper("testParallel").parallel(
                (input, step) -> {
                    try {
                        Thread.sleep(1000 * 5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return input + " p1";
                },
                (input, step) -> input + " p2"
        ).run("xx");
        System.out.println("=====" + r);
    }


    @Test
    void testContext() throws Exception {
        new TaskContext()
            // 任务task1: 等待条件
            .addTask(new TaskWrapper("task1").step((param, step) -> {
                step.info("执行 step{}, 检查属性 xxx: {}", step.num, step.ctx().getAttr("xxx"));
                return null;
            }).step((param, step) -> {
                step.info("执行 step{}, 检查属性 xxx: {}", step.num, step.ctx().getAttr("xxx"));
                return null;
            }, step -> step.ctx().getAttr("xxx") != null)) //当前步骤执行条件

            // 任务task2: 设置条件,然后恢复task1
            .addTask(new TaskWrapper("task2").step((param, step) -> {
                step.info("执行 step1, 设置属性");
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                step.ctx().setAttr("xxx", "ooo");
                step.ctx().resumeTask("task1"); // 设置属性xxx, 然后恢复task1继续执行
                return null;
            }))
            .start();
        Thread.sleep(1000 * 60);
    }


    @Test
    void deriveTask() {
        new TaskContext() //创建任务容器
            // 添加task1
            .addTask(new TaskWrapper("task1"))
            // 添加task2
            .addTask(new TaskWrapper("task2"))
            // 添加任务3: 在任务中衍生任务task4
            .addTask(new TaskWrapper("task3").step((param, step) -> {
                step.info("执行");
                step.ctx().addTask(new TaskWrapper("task4").step((param1, step1) -> {
                    step1.info("执行衍生任务");
                    return null;
                }));
                return null;
            }))
            .start();
    }
}
