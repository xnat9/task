# 介绍
轻量级任务管理

> **TaskContext**: _执行上下文/执行容器: 存放一组逻辑相关性的***TaskWrapper***_
> + 添加任务 => 等待对列 => 循环取出任务(直到并行限制 或 某个任务结束) => 正在执行对列 => 结束 
> > **TaskWrapper**: _任务由多个执行单元***Step***顺序组成_
> > + 入参 => Step1 => 结果(入参) => Step2 => 结果(入参) => Step3 => 结果
> > > **Step**: _任务执行单元/执行函数/执行步骤_
> > > + 一般步骤: 执行 => 结果
> > > + 可重复步骤: 执行 <=> 执行次数+1 <=> 是否需要重新执行 => 结果


# 安装教程
```xml
<dependency>
    <groupId>cn.xnatural.task</groupId>
    <artifactId>task</artifactId>
    <version>1.0.1</version>
</dependency>
```

# 用法
## 创建独立任务
```java
Object result = new TaskWrapper("task1")
    // 添加执行步骤1
    .step((param, me) -> {
        me.info("执行 step1 ... ");
        return param + "xxx";
    })
    // 添加执行步骤2
    .step((param, me) -> {
        me.info("执行 step2 ... ");
        return param + "ooo";
    }) // 可继续添加更多步骤
    .run("谁正在 ");
```

## 重复执行步骤任务
```java
new TaskWrapper()
    // 添加可重复执行步骤
    .reStep(3, (param, me) -> {
        me.info("执行可重复步骤 第 {} 次", me.times());
        return null;
    }, (result, me) -> { // 返回 true 则重复继续执行当前步骤
        if (result == null && me.times() < 3) return true;
        else return false;
    })
    .run()
```

## 任务暂停/恢复
```java
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, me) -> {
        me.info("执行 step1");
        me.task().suspend(); // 暂停下一个步骤执行
        return "xxx";
    })
    // 添加执行步骤2
    .step((param, me) -> {
        me.debug("执行 step2, 参数: " + param); // param 为xxx
        return null;
    });
task.run(); // 开始执行
Thread.sleep(1000L * 5);
task.resume(); // 恢复执行
```

## 一组相关任务(执行上下文/任务调度/执行容器)
```java
TaskContext ctx = new TaskContext(); //创建任务容器
// 添加task1
ctx.addTask(new TaskWrapper("task1"));
// 添加task2
ctx.addTask(new TaskWrapper("task2"));
// 添加任务3: 在任务中衍生任务task4
ctx.addTask(new TaskWrapper("task3").step((param, me) -> {
    me.info("执行");
    ctx.addTask(new TaskWrapper("task4").step((param, me) -> {
        me.info("执行衍生任务");
        return null;
    }));
    return null;
}));
ctx.start();
// ctx.suspend(); //暂停
// ctx.resume(); //恢复
```

## 异步恢复某个任务
```java
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
    me.info("设置属性 xxx:ooo");
    ctx.setAttr("xxx", "ooo");
    ctx.resumeTask("task1"); // 设置属性xxx, 然后恢复task1继续执行
    return null;
}));
```

# 参与贡献
xnatural@msn.cn
