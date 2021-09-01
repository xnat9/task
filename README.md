# 介绍
轻量级任务编排管理

> **TaskContext**: _执行上下文/执行容器: 存放一组逻辑相关性的***TaskWrapper***_
> + 添加任务 => 等待对列 => 循环取出任务(直到并行限制 或 某个任务结束) => 正在执行对列 => 结束 
> > **TaskWrapper**: _任务由多个执行单元***Step***顺序组成_
> > + 入参 => Step1 => 结果(入参) => Step2 => 结果(入参) => Step3 => 结果
> > > **Step**: _任务执行单元/执行函数/执行步骤_
> > > + 一般步骤: 执行 => 结果
> > > + 并行步骤: 多个执行函数并行
> > > + 可重复步骤: 执行 <=> 执行次数+1 <=> 是否需要重新执行 => 结果


# 安装教程
```xml
<dependency>
    <groupId>cn.xnatural.task</groupId>
    <artifactId>task</artifactId>
    <version>1.0.2</version>
</dependency>
```

# 任务基本用法
## 创建独立任务
```java
Object result = new TaskWrapper("task1")
    // 添加执行步骤1
    .step((param, step) -> {
        step.info("执行 step{} ... ", step.num);
        return param + "xxx";
    })
    // 添加执行步骤2
    .step((param, step) -> {
        step.info("执行 step{} ... ", step.num);
        return param + "ooo";
    }) // 可继续添加更多步骤
    .run("谁正在 ");
```

## 重复执行步骤任务
```java
Object result = new TaskWrapper()
    // 添加可重复执行步骤
    .reStep(5, (input, step) -> {
        step.info("执行重试step{}, 第 {} 次", step.num, step.times());
        return new Random().nextInt(10) % 2 == 0 ? null : input + " end";
    }, (result, step) -> { // 返回 true 则重复继续执行当前步骤
        if (result == null) return true;
        else return false;
    })
    .run("xxx");
```

## 并发执行步骤任务
```java
Object result = new TaskWrapper()
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
    .run()
```

## 任务暂停/恢复
### 主动暂停
```java
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, step) -> {
        step.info("执行 step1");
        step.task().suspend(); // 暂停下一个步骤执行
        return "xxx";
    })
    // 添加执行步骤2
    .step((param, step) -> {
        step.debug("执行 step2, 参数: " + param); // param 为xxx
        return null;
    });
task.run(); // 开始执行
Thread.sleep(1000L * 5);
task.resume(); // 恢复执行
```

### 条件步骤
```java
AtomicInteger var = new AtomicInteger();
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, step) -> {
        step.info("执行 step{}", step.num);
        return "xxx";
    })
    // 条件步骤
    .step((param, step) -> {
        step.info("执行 step{} ... ", step.num);
        return "xxx";
    }, step -> var.get() > 0); // 当前步骤执行的条件判断函数
task.run(); // 开始执行
Thread.sleep(1000L * 5);
var.incrementAndGet();
task.resume(); // 恢复执行
Thread.sleep(1000L * 5);
```

# 任务容器
> 一个大任务被拆分为多个小任务时

> 一组相关任务(执行上下文/任务调度/执行容器), 可编排复杂任务执行顺序规则

> 可控制任务并发数

> 当容器中两个队列(正在执行队列,等待执行队列)都为空并且是非暂停状态时, 容器自动结束

## 创建简单任务容器
```java
new TaskContext("ctx1") //创建任务容器
    // 添加任务1
    .addTask(new TaskWrapper("task1"))
    // 添加任务2
    .addTask(new TaskWrapper("task2"))
    // 添加任务3: 在任务中衍生任务任务4
    .addTask(new TaskWrapper("task3").step((param, step) -> {
        step.info("执行 step{}", step.num);
        return null;
    }).step((input, step) -> {
        // 任务中继续添加任务到当前任务容器
        step.ctx().addTask(new TaskWrapper("task4").step((input1, step1) -> {
            step1.info("执行衍生任务");
            return null;
        }));
    }))
    .start();
```

## 异步恢复某个任务
```java
new TaskContext("ctx2")
    // 任务1: 等待条件
    .addTask(new TaskWrapper("task1").step((param, step) -> {
        step.info("执行 step1, 检查属性 xxx: {}", step.ctx().getAttr("xxx"));
        return null;
    }).step((param, step) -> {
        step.info("执行 step2, 检查属性 xxx: {}", step.ctx().getAttr("xxx"));
        return null;
    }, step -> step.ctx().getAttr("xxx") != null)) //当前步骤执行条件
    
    // 任务2: 设置条件,然后恢复task1
    .addTask(new TaskWrapper("task2").step((param, step) -> {
        step.info("设置属性 xxx:ooo");
        step.ctx().setAttr("xxx", "ooo");
        step.ctx().resumeTask("task1"); // 设置属性xxx, 然后恢复task1继续执行
        return null;
    }))
    .start();
```


# 1.0.3 ing...

# 参与贡献
xnatural@msn.cn
