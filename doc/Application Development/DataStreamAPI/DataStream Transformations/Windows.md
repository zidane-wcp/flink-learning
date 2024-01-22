## Overview

窗口（Window）是无界流处理的核心。窗口将无界的流分割成具有有限大小的“桶”，在这之上我们可以做计算和处理。本节介绍Flink中如何使用窗口。

窗口化Flink程序的一般结构如下所示。第一段说的是keyed stream，`window()`只能应用在`KeyedStream`上，所以先对流进行keyby操作，生成KeyedStream，然后做window操作。第二段说的是non-keyed stream，可以作用在`DataSteam`上，但是开窗时需要用`windowAll()`方法。

**Keyed Windows**

```
stream
       .keyBy(...)               <-  keyed versus non-keyed windows
       .window(...)              <-  required: "assigner"
      [.trigger(...)]            <-  optional: "trigger" (else default trigger)
      [.evictor(...)]            <-  optional: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
       .reduce/aggregate/apply()      <-  required: "function"
      [.getSideOutput(...)]      <-  optional: "output tag"
```

**Non-Keyed Windows**

```
stream
       .windowAll(...)           <-  required: "assigner"
      [.trigger(...)]            <-  optional: "trigger" (else default trigger)
      [.evictor(...)]            <-  optional: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
       .reduce/aggregate/apply()      <-  required: "function"
      [.getSideOutput(...)]      <-  optional: "output tag"
```

## Window Lifecycle

简而言之，一旦应该属于该窗口的第一个元素到达，就会创建一个窗口。当时间（event time或processing time）超过窗口的结束时间+用户指定的延迟时间（Allowed Lateness）时，窗口将会完全移除。Flink仅保证删除基于时间的窗口，而不保证删除其他类型的窗口，比如global windows。例如，想要每5分钟创建一个基于event time的非重叠的窗口（就是滑动窗口 tumbling window），并允许1分钟的延迟，对于12：00到12：05的这个窗口来说，当属于该时间范围的第一个元素达到时，该窗口就会被创建，当水位线（watermark）超过12:06时，该窗口就会被移除。

此外，每个窗口还将附加一个触发器（Trigger）、一个窗口函数（Window Function）（比如 `ProcessWindowFunction`, `ReduceFunction`,  `AggregateFunction`等）。窗口函数指定了要在窗口内的数据做的计算或处理，而触发器则指定了应用该窗口函数的触发条件，即何时触发窗口函数的计算。窗口的触发策略可能类似于“当窗口中的元素数量超过4时”，或“当水位线经过窗口末尾时”。触发器还可以决定在创建和删除窗口之间的任何时间清楚窗口的数据，此处的清除只清除窗口内的元素，而不会清除窗口的元数据。这意味着新的数据仍然可以进入该窗口。

另外，你还可以指定一个驱逐器（Evictor），它能在窗口触发后，而且应用窗口函数之前或之后的时间范围内，删除窗口内的元素。

从以上内容可知，Flink中的window一般分为四个组件（components）：

* Window Assigner：将元素分配到正确的窗口内。
* Window Function：定义了窗口内元素的处理方式。
* Window Trigger：定义了在窗口上应用窗口函数的时机。
* Window Evictor：定义了窗口内元素的删除时机。

本节接下来的内容将会详细介绍上述窗口组件。

## Keyed vs Non-Keyed Windows

对于窗口操作，首先要确定数据流是不是`KeyedStream`。使用`keyBy()`算子可以将无界数据流分割成逻辑上分组的数据流（`KeyedStream`）。

对于keyed stream来说，输入事件的每个属性（字段）都可以作为key（keyBy()中指定）。`KeyedStream`允许在多个执行线程上并行的进行窗口计算，因为每一个逻辑keyedStream都可以独立于其他的KeyedStream进行处理，但是所有具有相同key的元素会在同一个并发task中处理。

对于non-keyed stream来说，初始数据流不会被分割成逻辑的数据流，所有的窗口计算在同一个task上执行，就像并发度为1一样。

## Window Assigners

在确定初始数据流是否为`KeyedStream`后，下一步就需要定义*window assigner*（窗口分配器）了。窗口分配器定义了数据流中的元素如何分配给各个windows。可以通过`window()`（对于keyed stream）或者`windowAll()`（对于non-keyed stream）指定窗口分配器。

```java
public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner)
```

Flink内置的窗口分配器适用于大多数场景，内置分配器有*tumbling windows、sliding windows、session windows、global windows*。你也可以继承`WindowAssigner`类实现一个自定义的窗口分配器。所有的内置分配器（除了global windows）都是基于时间向窗口分配数据，可以是processing time，也可以是event time。

Flink源码中的TimeWindow类是基于时间窗口的实现类，其有*start*和*end*两个属性，表示窗口的开始时间和结束时间，另外也提供了查询开始时间和结束时间的方法`getStart()、getEnd()`，以及`axTimestamp()`，用以返回窗口内允许的最大时间戳（该方法返回值为end-1）。

### Tumbling Windows

滑动窗口分配器（tumbling windows assigner）将元素分配给具有指定大小的窗口，滑动窗口的大小是固定的，且不会互相重合。比如，如果指定了一个5分钟的滑动窗口，则将评估当前窗口，并每5分钟创建一个新的窗口，如下图所示。

![image-20240122185023157](https://nightlies.apache.org/flink/flink-docs-release-1.15/fig/tumbling-windows.svg)

