### 介绍
轻量级任务管理

Step(步骤): 任务执行单元

TaskWrapper(任务): 一组顺序相关性的步骤组成

TaskContext(执行上下文/任务调度/执行容器): 一组逻辑相关性的任务
 * 并行任务
 * 衍生任务
 * 任务调度

### 安装教程
```xml
<dependency>
    <groupId>cn.xnatural.task</groupId>
    <artifactId>task</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 用法
#### 创建独立任务
```java
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, me) -> {
        me.info("执行 step1 ... ");
        return "xxx";
    }); // 可继续添加n个执行步骤
task.start(); // 开始执行

// 自定义任务标识
new TaskWrapper("task1")
```

#### 重复执行步骤任务
```java
TaskWrapper task = new TaskWrapper()
    // 添加可重复执行步骤
    .reStep(3, (param, me) -> {
        me.info("执行可重复步骤 第 {} 次", me.times());
        return null;
    }, (param, me) -> { // 返回 true 则重复继续执行当前步骤
        if (param == null && me.times() < 3) return true;
        else return false;
    });
task.start(); // 开始执行
```

#### 任务暂停/恢复
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
    });;
task.start(); // 开始执行
Thread.sleep(1000L * 5);
task.resume(); // 恢复执行
```

#### 一组相关任务(执行上下文/任务调度/执行容器)
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
// ctx.suspend(); //暂停
// ctx.resume(); //恢复
ctx.start();
```

#### 异步恢复某个任务
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
    me.info("执行 step1, 设置属性");
    ctx.setAttr("xxx", "ooo");
    ctx.resumeTask("task1"); // 设置属性xxx, 然后恢复task1继续执行
    return null;
}));
```

### 参与贡献
xnatural@msn.cn
