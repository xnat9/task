import cn.xnatural.task.TaskContext;
import cn.xnatural.task.TaskWrapper;
import org.junit.jupiter.api.Test;

public class TaskTest {

    public static void main(String[] args) throws Exception {
        TaskContext<TaskWrapper> tCtx = new TaskContext<>();
        tCtx.addTask(new TaskWrapper().step((i, me) -> {
            me.log().info("执行xxx");
            return "xxx";
        }).step((i, me) -> {
            me.log().info("执行第2步. 参数为: " + i);
            return "ooo";
        }).step((i, me) -> {
            TaskWrapper t = new TaskWrapper().step((ii, tt) -> {
                tt.log().info("执行步骤...............");
                return null;
            });
            me.log().info("生成新的任务: " + t.getKey());
            tCtx.addTask(t);
            return t;
        }));
        tCtx.start();
    }


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
                }, (param, me) -> {
                    if (param == null && me.times() < 3) return true;
                    else return false;
                });
        task.start();
        Thread.sleep(1000L * 5);
        task.resume(); // 恢复执行
    }


    @Test
    void testContext() throws Exception {
        TaskContext ctx = new TaskContext();
        ctx.addTask(new TaskWrapper().step((param, me) -> {
            me.info("");
            return null;
        }));
        ctx.start();
        Thread.sleep(1000L * 5);
        ctx.resume(); // 恢复执行
    }


}
