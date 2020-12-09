#### 介绍
轻量级任务管理

TaskWrapper任务: 一组顺序相关性的步骤组成

TaskContext(执行的上下文/管理者/执行容器): 一组逻辑相关性的任务
 * 并行任务
 * 衍生任务
 * 任务调度

#### 创建独立任务
```
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, me) -> {
        me.info("执行 step1 ... ");
        return "xxx";
    }); // 可继续添加n个执行步骤
task.start(); // 开始执行
```

#### 重复执行步骤任务
```
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .reStep(3, (param, me) -> {
        me.info("执行 重试 step3, 第 {} 次", me.times());
        return null;
    }, (param, me) -> { // 返回 true 则重复继续执行当前步骤
        if (param == null && me.times() < 3) return true;
        else return false;
    });
task.start(); // 开始执行
```

#### 任务暂停/恢复
```
TaskWrapper task = new TaskWrapper()
    // 添加执行步骤1
    .step((param, me) -> {
        me.info("执行 step1 ... ");
        me.task().suspend(); // 暂停下一个步骤执行
        return "xxx";
    })
    // 添加执行步骤2
    .step((param, me) -> {
        me.debug("执行 step2 ... . 参数: " + param); // param 为xxx
        return null;
    });;
task.start(); // 开始执行
Thread.sleep(1000L * 5);
task.resume(); // 恢复执行
```

#### 一组相关任务
```
TaskContext ctx = new TaskContext(); //创建任务容器
// 添加任务1
ctx.addTask(new TaskWrapper().step((param, me) -> {
    me.info("执行任务1");
    return null;
}));
// 添加任务2
ctx.addTask(new TaskWrapper().step((param, me) -> {
    me.info("执行任务2");
    return null;
}));
// 添加任务3: 在任务中衍生任务
ctx.addTask(new TaskWrapper().step((param, me) -> {
    me.info("执行任务2");
    ctx.addTask(new TaskWrapper().step((param, me) -> {
        me.info("执行衍生任务");
        return null;
    }));
    return null;
}));
ctx.start();
```

#### 参与贡献
xnatural@msn.cn
