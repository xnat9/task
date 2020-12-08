import cn.xnatural.task.TaskWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTest {

    public static void main(String[] args) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "aio-" + i.getAndIncrement());
            }
        });
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("delimiter", "\n");
        new TaskWrapper().step((i, task) -> {
            task.log().info("执行xxx");
            return "xxx";
        }).step((i, task) -> {
            task.log().info("执行第2步. 参数为: " + i);
            return "ooo";
        }).start();

        // exec.shutdown();
    }
}
