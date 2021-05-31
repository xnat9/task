import cn.xnatural.task.TaskContext;
import cn.xnatural.task.TaskWrapper;
import org.junit.jupiter.api.Test;

public class TaskTest {

    @Test
    void taskTest() throws Exception {
        TaskWrapper task = new TaskWrapper()
                .step((param, me) -> {
                    me.info("执行 step1 ... ");
                    me.task().suspend();
                    return "xxx";
                })
                .step((param, me) -> {
                    me.debug("执行 step2 ... . 参数: " + param);
                    return null;
                })
                .reStep(3, (param, me) -> {
                    me.info("执行 重试 step3, 第 {} 次", me.times());
                    return null;
                }, (result, me) -> {
                    if (result == null && me.times() < 3) return true;
                    else return false;
                });
        task.run();
        Thread.sleep(1000L * 5);
        task.resume(); // 恢复执行
    }


    @Test
    void emptyStepTask() {
        new TaskWrapper("empty").run();
    }


    @Test
    void testContext() throws Exception {
        TaskContext ctx = new TaskContext();
        // 任务task1: 等待条件
        ctx.addTask(new TaskWrapper("task1").step((param, me) -> {
            me.info("执行 step1, 检查属性 xxx: {}", ctx.getAttr("xxx"));
            if (ctx.getAttr("xxx") == null) me.task().suspend(); // 属性为空,则暂停
            return null;
        }).step((param, me) -> {
            me.info("执行 step2, 检查属性 xxx: {}", ctx.getAttr("xxx"));
            return null;
        }));

        // 任务task2: 设置条件,然后恢复task1
        ctx.addTask(new TaskWrapper("task2").step((param, me) -> {
            me.info("执行 step1, 设置属性");
            ctx.setAttr("xxx", "ooo");
            ctx.resumeTask("task1"); // 设置属性xxx, 然后恢复task1继续执行
            return null;
        }));
        ctx.start();
        ctx.suspend();
        Thread.sleep(1000 * 5);
        ctx.resume();
    }


    @Test
    void deriveTask() {
        TaskContext ctx = new TaskContext(); //创建任务容器
        // 添加task1
        ctx.addTask(new TaskWrapper("task1"));
        // 添加task2
        ctx.addTask(new TaskWrapper("task2"));
        // 添加任务3: 在任务中衍生任务task4
        ctx.addTask(new TaskWrapper("task3").step((param, step) -> {
            step.info("执行");
            ctx.addTask(new TaskWrapper("task4").step((param1, step1) -> {
                step1.info("执行衍生任务");
                return null;
            }));
            return null;
        }));
        ctx.start();
    }
}
