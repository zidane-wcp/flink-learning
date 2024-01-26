## Overview

> 本章内容中的窗口函数的源码位于[org.apache.flink.streaming.api.functions.windowing](https://github.com/apache/flink/tree/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/windowing)包中，其他源码都位于[org.apache.flink.streaming.api.windowing](https://github.com/apache/flink/tree/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing)包中。

窗口（Window）是无界流处理的核心，它将无界的流分割成具有有限大小的“桶”，在这之上我们可以做计算和处理。本节介绍Flink中的窗口计算。

Flink中窗口计算程序的结构大概如下所示。可以看到，Flink的窗口计算既可以应用在`KeyedStream`上，也可以应用在`DataStream`上，但是开窗时需要调用不同的方法，前者需要调用`window(...)`方法， 而后者需要调用`windowAll()`方法。

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

简而言之，一旦应该属于该窗口的第一个元素到达，就会创建一个窗口。当时间（event-time或processing-time）超过窗口的结束时间+用户指定的延迟时间（Allowed Lateness）时，窗口将会完全移除。Flink仅保证删除基于时间的窗口，而不保证删除其他类型的窗口，比如global window（Flink中只有两种窗口类型，`TimeWindow`和`GlobalWindow`）。例如，创建一个大小为5分钟、基于event time的非重叠的窗口（就是滑动窗口 tumbling window），并允许1分钟的延迟，对于12：00到12：05的这个窗口来说，当属于该时间范围的第一个元素达到时，该窗口就会被创建，当水位线（watermark）超过12:06时，该窗口就会被移除。

此外，每个窗口还将附加一个触发器（Trigger）、一个窗口函数（Window Function）（比如 `ProcessWindowFunction`, `ReduceFunction`,  `AggregateFunction`等）。窗口函数指定了要在窗口内的数据做的计算或处理，而触发器则指定了应用该窗口函数的触发条件，即何时触发窗口函数的计算。窗口的触发策略可能类似于“当窗口中的元素数量超过4时”，或“当watermark经过窗口末尾时”。触发器还可以决定在创建和删除窗口之间的任何时间清除窗口的数据，此处的清除只清除窗口内的元素，而不会清除窗口的元数据。这意味着新的数据仍然可以进入该窗口。

另外，你还可以指定一个驱逐器（Evictor），它能在窗口触发后，而且应用窗口函数之前或之后的时间范围内，删除窗口内的元素。

从以上内容可知，Flink中的window模型包括四个组件（components）：

* Window Assigner：将元素分配到正确的窗口内。
* Window Function：定义了窗口内元素的处理方式。
* Window Trigger：定义了在窗口上应用窗口函数的时机。
* Window Evictor：定义了窗口内元素的删除时机。

本节接下来的内容将会详细介绍上述窗口组件。

## Keyed vs Non-Keyed Windows

对于窗口操作，首先要确定数据流是不是`KeyedStream`。使用`keyBy()`算子可以将无界数据流分割成逻辑上分组的数据流（`KeyedStream`）。

对于keyed stream来说，输入事件的每个属性或字段都可以作为key（`keyBy()`中指定）。`KeyedStream`允许在多个执行线程上并行的进行窗口计算，因为每一个拥有不同key的逻辑keyedStream都可以独立于其他的KeyedStream进行处理，但是所有具有相同key的元素会在同一个并发task中处理。

对于non-keyed stream来说，初始数据流不会被分割成逻辑的数据流，所有的窗口计算在同一个task上执行，就像并发度为1一样。

## Window Operators(补充)

Flink的window模型提供了一些算子（方法），用于为数据流指定不同的窗口操作：

* `window()`，为`KeyedStream`指定窗口分配器，需要传入一个`WindowAssigner`对象，返回`WindowedStream`对象。
* `windowAll()`，为`DataStream`指定窗口分配器，需要传入一个`WindowAssigner`，返回`AllWindowedStream`对象。

* `reduce()`，为`WindowedStream`或`AllWindowedStream`指定窗口函数，需要传入一个`ReduceFunction`对象，以及另一个可选的`ProcessWindowFunction`对象（如果需要将`ReduceFunction`与`ProcessWindowFunction`结合使用），返回`SingleOutputStreamOperator`对象。
* `aggregate()`为`WindowedStream`或`AllWindowedStream`指定窗口函数，需要传入一个`AggregationFunction`对象，以及另一个可选的`ProcessWindowFunction`对象（如果需要将`AggregationFunction`与`ProcessWindowFunction`结合使用），返回`SingleOutputStreamOperator`对象。
* `process()`，为`WindowedStream`或`AllWindowedStream`指定窗口函数，需要传入一个`ProcessWindowFunction`对象，返回`SingleOutputStreamOperator`对象。
* `apply()`，为`WindowedStream`或`AllWindowedStream`指定窗口函数，需要传入一个`WindowFunction`对象，返回`SingleOutputStreamOperator`对象。注意`WindowFunction`已经过时，所以`apply()`算子基本不用，可用`process()`替换。

* `trigger()`，为`WindowedStream`或`AllWindowedStream`指定触发器，需要传入一个`Trigger`对象，返回`WindowedStream`或`AllWindowedStream`。

* `evictor()`，为`WindowedStream`或`AllWindowedStream`指定驱逐器，需要传入一个`Evictor`对象，返回`WindowedStream`或`AllWindowedStream`。
* `allowedLateness()`，为`WindowedStream`或`AllWindowedStream`指定允许元素延迟的最大时间，需要传入一个`Time`对象，返回`WindowedStream`或`AllWindowedStream`。
* `sideOutputLateData()`，为`WindowedStream`或`AllWindowedStream`中迟到的但未删除的元素指定旁路输出的tag，使用该tag可以将这些迟到但未删除的元素收集到一个`DataStream`对象中，需要传入一个`Time`对象，返回`WindowedStream`或`AllWindowedStream`。
* `getSideOutput()`，从窗口函数输出的数据流`SingleOutputStreamOperator`对象中（所以该算子需要应用在窗口函数之后），获取那些迟到的但未删除的元素，需要传入一个`OutputTag`对象，返回`DataStream`对象。

需要注意的是，以上的`WindowFunction`和`ProcessWindowFunction`基本可以互换使用，例如，`reduce()`也可以传入一个`ReduceFunction`和一个`WindowFunction`对象，用于这俩的结合使用。但是`WindowFunction`是老版本的窗口函数，现在基本都用`ProcessWindowFunction`。

除了以上这些基本算子之外，Flink还提供了一些封装过的窗口函数算子，底层都是基于`reduce()`算子实现，使用`ReduceFunction`进行计算，使用起来更简单方便：

* `max()`，可以应用在`WindowedStream`或`AllWindowedStream`之上，用于返回窗口中拥有指定字段处最大值的元素，可以传入`int positionToMax`或者是`String field`，返回`SingleOutputStreamOperator`数据流对象，该数据流中的元素数据类型与输入元素保持一致（因为底层基于`reduce()`）。
* `maxBy()`，可以应用在`WindowedStream`或`AllWindowedStream`之上，用于返回窗口中拥有指定字段处最大值的元素，可以传入`int positionToMax`或者是`String field`，返回`SingleOutputStreamOperator`数据流对象，该数据流中的元素数据类型与输入元素保持一致（因为底层基于`reduce()`）。
* `min()`，与`max()`类似。
* `minBy()`，与`maxBy`类似。
* `sum()`，可以应用在`WindowedStream`或`AllWindowedStream`之上，用于返回窗口中元素指定字段的累加值，可以传入`int positionToMax`或者是`String field`，返回`SingleOutputStreamOperator`数据流对象，该数据流中的元素数据类型与输入元素保持一致（因为底层基于`reduce()`）。

需要注意的是，看起来`max()`与`maxBy()`是一样的，他俩确实有很多共同点，例如，因为都是基于`reduce()`算子，所以都是增量聚合；都会根据代码中的逻辑，更新状态中记录的聚合中间值，并输出。但是他俩也有个不同点，就是在更新状态中记录的聚合中间值时，`max()`只会更新指定字段的最大值，正如字面意思所表达，只计算指定字段的最大值，不关注其他字段；而`maxBy()`则会更新整条数据。

另外，`sum()`算子在更新状态中记录的聚合中间值时，是跟`max()`和`min()`一样的，只更新指定字段的累加值，不关注其他字段。

可以结合配套代码，执行并查看结果，更容易理解。

## Window Assigners

在确定初始数据流是否为`KeyedStream`后，下一步就需要定义*window assigner*（窗口分配器）了。窗口分配器定义了数据流中的元素如何分配给各个windows。可以通过`window()`（对于keyed stream）或者`windowAll()`（对于non-keyed stream）指定窗口分配器。

```java
public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner)
```

Flink内置的窗口分配器可以适用于大多数场景，包括*tumbling windows、sliding windows、session windows、global windows*。你也可以继承`WindowAssigner`类实现一个自定义的窗口分配器。

>  Flink中有两种类型的窗口，一种是`TimeWindow`，另一种是`GlobalWindow`。
>
> `TimeWindow`是基于时间的，可以是processing-time，也可以是event-time，其有*start*和*end*两个属性，表示窗口的开始时间和结束时间（左闭右开区间），另外也提供了查询开始时间和结束时间的方法`getStart()、getEnd()`，以及`maxTimestamp()`，用以返回窗口内允许的最大时间戳，其返回值为end-1，所以窗口的时间范围是[start, end)，左闭右开区间。
>
> 而`GlobalWindow`是一种全局窗口，其`maxTimestamp()`方法返回值为`Long.MAX_VALUE`，也就是说这种窗口没有结束时间。

其中，前三种窗口分配器都是基于时间向窗口分配数据，其分配数据的窗口也必须是`TimeWindow`类型，而时间可以是processing-time，也可以是event-time。第四种global windows是全局窗口分配器，其分配数据的窗口必须是`GlobalWindow`类型。

### Tumbling Windows

> 有两个实现类：`TumblingEventTimeWindows`和`TumblingProcessingTimeWindows`。

滑动窗口分配器（tumbling windows assigner）将元素分配给具有指定大小的窗口，滑动窗口的大小是固定的，且不会互相重合。比如，如果指定了一个5分钟的滑动窗口，则将评估当前窗口，并每5分钟创建一个新的窗口，如下图所示。

![Tumbling Windows](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/tumbling-windows-20240122230907184-5936154-5936157.svg)

```java
DataStream<T> input = ...;

// tumbling event-time windows
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// tumbling processing-time windows
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// daily tumbling event-time windows offset by -8 hours.
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```

对于时间间隔的指定，可以用`Time.milliseconds(x)、Time.seconds(x)`等等。

除了时间间隔参数，滑动窗口分配器还有个可选的参数偏移量`offset`，可用于更改窗口的对齐方式。例如，如果没有偏移量，每小时的滑动窗口将与自然时间对其，也就是说窗口的时间范围是类似于 `1:00:00.000 - 1:59:59.999`、`2:00:00.000 - 2:59:59.999`的。相反，如果设置了偏移量，比如设置15分钟，则窗口的时间范围为`1:15:00.000 - 2:14:59.999`、`2:15:00.000 - 3:14:59.999`。offset一个很重要的使用场景是将窗口调整为UTC-0以外的时区，比如在UTC+8时区，你必须指定offset为`Time.hours(-8)`。

### Sliding Windows

> 有两个实现类：`SlidingEventTimeWindows`和`SlidingProcessingTimeWindows`。

滑动窗口分配器（sliding windows assigner）将元素分配到固定大小的窗口内。类似于滚动窗口，可以配置窗口的大小。滑动窗口另一个参数是窗口滑动步长（window slide），用于控制滑动窗口启动的频率。因此，如果滑动步长小于窗口大小，则滑动窗口就会重叠，这时元素就会分配给多个窗口。而如果窗口大小等于步长，则会得到一个滚动窗口，滚动窗口是滑动窗口的一种特殊情况。

例如，某个滑动窗口大小为10分钟，步长为5分钟，这样每5分钟就会得到一个窗口，包含过去10分钟内的事件，如下图所示。

![sliding windows](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/sliding-windows.svg)

```java
DataStream<T> input = ...;

// sliding event-time windows
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows offset by -8 hours
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```

对于时间间隔的指定，可以用`Time.milliseconds(x)、Time.seconds(x)`等等。

滑动窗口分配器同样接受可选参数`offset`，也是用来控制窗口的对齐方式。例如，窗口大小为1小时，步长为30分钟，不设置offset，则会得到如下窗口`1:00:00.000 - 1:59:59.999`、`1:30:00.000 - 2:29:59.999`等等。而如果设置offset为15分钟，则会得到如下窗口`1:15:00.000 - 2:14:59.999`、`1:45:00.000 - 2:44:59.999`等等。offset一个很重要的使用场景是将窗口调整为UTC-0以外的时区，比如在UTC+8时区，你必须指定offset。

### Session Windows

> 有四个实现类：`DynamicEventTimeSessionWindows`、`DynamicProcessingTimeSessionWindows`、`EventTimeSessionWindows`、`ProcessingTimeSessionWindows`。

会话窗口分配器（session windows assigner）会按照活跃的会话对元素进行分组。与滚动窗口与滑动窗口相比，会话窗口不会重叠，也不会有固定的开始和结束时间。相反，当会话窗口在一个时间段内没有接收到元素时（即当发生不活跃间隙时），窗口就会关闭。会话窗口分配器可以配置静态会话间隙，也可以配置为会话间隙提取器，它将定义非活跃间隙的时长，当该时长到期时，当前会话将关闭，后续元素将分配给新的会话窗口。

![session windows](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/session-windows-5938960.svg)

```java
DataStream<T> input = ...;

// event-time session windows with static gap
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);
    
// event-time session windows with dynamic gap
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withDynamicGap((element) -> {
        // determine and return session gap
    }))
    .<windowed transformation>(<window function>);

// processing-time session windows with static gap
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);
    
// processing-time session windows with dynamic gap
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withDynamicGap((element) -> {
        // determine and return session gap
    }))
    .<windowed transformation>(<window function>);
```

静态会话间隙可以通过`Time.milliseconds(x)、Time.seconds(x)、Time.minutes(x)`等指定。动态会话间隙可以通过实现`SessionWindowTimeGapExtractor`接口来指定。

因为会话窗口没有固定的开始时间和结束时间，因此其评估方式与滚动窗口和滑动窗口是不同的。底层原理方面，会话窗口算子为每个到来的元素创建一个新的窗口，如果这些窗口的间隔小于我们定义的会话间隙，则会合并这些窗口。为了能够进行合并，会话窗口算子还需要合并触发器（merging Trigger）和合并窗口函数（Window Function），例如`ReduceFunction、AggreateFunction、ProcessWindowFunction`等。

### Global Windows

> 有一个实现类：`GlobalWindows`。

全局窗口分配器（global windows assigner）将具有相同key的所有元素分配到同一个全局窗口内。因为全局窗口没有结束时间，而且其默认触发器为`NeverTrigger`，所以若想在`GlobalWindow`窗口上触发计算，必须指定自定义的触发器。

![global windows](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/non-windowed-5978343.svg)

```java
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>);
```

## Window Functions

在指定了窗口分配器（window assigner）后，接下来需要指定要在每个窗口上执行的计算。这就是窗口函数（window function）的职责，一旦系统确定窗口已经准备好进行计算，该函数就应用在窗口中的每个元素上（其中，触发器决定了一个窗口何时准备好）。

窗口函数可以是`ReduceFunction、AggregateFunction、ProcessWindowFunction`。前两个执行效率更高，因为其对窗口内每个到达的元素进行增量聚合。而`ProcessWindowFunction`会获取每个窗口内所有元素的迭代器（会对窗口内所有元素进行一次全量计算）以及元素所属窗口的额外元数据信息。

使用`ProcessWindowFunction`的窗口算子不能像其他窗口函数那样高效地执行，因为Flink在调用窗口函数之前必须在内部缓冲窗口内的所有元素。而如果我们将`ProcessWindowFunction`与`ReduceFuncton、AggregateFunction`进行组合，我们既可以获得窗口元素的增量聚合能力，又可以得到`ProcessWindowFunction`中的窗口元数据信息。

### ReduceFunction

`ReduceFunction`需要通过`WindowedStream.reduce()`方法来指定，该窗口函数定义了两个具有相同数据类型的输入元素进行组合，并生成一个具有相同数据类型的输出元素。

Flink通过`ReduceFunction`实现了窗口内元素的增量聚合，其底层实现是将中间“合并的结果”作为状态保存起来，之后每来一个新的数据，就和之前的聚合状态进一步做归约，所以其窗口状态数据量可以非常小。缺点是能实现的功能比较有限，因为其中间状态的数据类型、输入类型、输出类型三者必须一致，而且窗口中只保存了一个中间状态数据，无法对窗口内所有数据进行操作。

以下程序示例对窗口内元素的第二个字段进行累加。

```java
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce(new ReduceFunction<Tuple2<String, Long>>() {
      public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
      }
    });
```

### AggregateFunction

`AggregateFunction`接口也是一个基于中间计算结果状态进行增量计算的窗口函数，通过`aggregate()`方法来指定。`AggregateFunction`比`ReduceFunction`更加通用，它有三种参数类型：一个输入类型（IN）、一个累加器类型（ accumulator type，ACC）、以及一个输出类型（OUT）。输入类型是输入流中元素的类型，AggregateFunction具有将一个输入元素添加到累加器的方法。该接口还有另外几个方法：创建初始累加器的方法、将两个累加器合并为一个累加器的方法、从累加器中抽取输出（OUT）的方法。

该接口中有四个方法：

* `ACC createAccumulator()`，创建初始累加器，要进行一些初始化的工作，比如我们要进行count计数操作，就需要给累加器一个初始值。
* `ACC add(IN var1, ACC var2)`，该方法用来定义做聚合时的核心逻辑，对窗口中新到来的元素与当前累加器进行聚合，然后返回聚合后的累加器。
* `OUT getResult(ACC var1)`，对累加器进行适当处理后，按照输出`OUT`的类型进行返回，返回的就是窗口聚合计算的结果。
* `ACC merge(ACC var1, ACC var2)`，按照合适的逻辑，合并两个累加器，并返回相同的累加器类型。由于Flink是一个分布式计算框架，计算可以分布在不同的节点上进行。例如上述`add()`方法可能在不同的节点上被调用，该方法只会对当前节点的数据进行聚合，那么当执行`getResult()`方法时，就需要对各个节点的累加器进行合并，否则结果就是局部的、不正确的。

以下示例展示如何用`AggregateFunction`计算平均值，其中，`accumulator.f0`存储数据流中的Long类型数据之和，`accumulator.f1`存储数据流中元素的个数。

```java

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
private static class AverageAggregate
    implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
  @Override
  public Tuple2<Long, Long> createAccumulator() {
    return new Tuple2<>(0L, 0L);
  }

  @Override
  public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
    return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
  }

  @Override
  public Double getResult(Tuple2<Long, Long> accumulator) {
    return ((double) accumulator.f0) / accumulator.f1;
  }

  @Override
  public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
  }
}

DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .aggregate(new AverageAggregate());
```

### ProcessWindowFunction

`ProcessWindowFunction`通过`process()`方法来指定，其可以获取包含窗口中所有元素的迭代器，以及一个`Context`对象，该对象可以用来访问时间和状态信息，这使得`ProcessWindowFunction`比其他窗口函数更加灵活。这是以性能和资源消耗为代价的，因为窗口中的元素不能增量聚合，反而需要缓存所有元素，直到窗口触发器触发窗口函数的计算。

以下代码段为`ProcessWindowFunction`抽象类的源码。

```java
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> implements Function {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     *
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public abstract void process(
            KEY key,
            Context context,
            Iterable<IN> elements,
            Collector<OUT> out) throws Exception;

   	/**
   	 * The context holding window metadata.
   	 */
   	public abstract class Context implements java.io.Serializable {
   	    /**
   	     * Returns the window that is being evaluated.
   	     */
   	    public abstract W window();

   	    /** Returns the current processing time. */
   	    public abstract long currentProcessingTime();

   	    /** Returns the current event-time watermark. */
   	    public abstract long currentWatermark();

   	    /**
   	     * State accessor for per-key and per-window state.
   	     *
   	     * <p><b>NOTE:</b>If you use per-window state you have to ensure that you clean it up
   	     * by implementing {@link ProcessWindowFunction#clear(Context)}.
   	     */
   	    public abstract KeyedStateStore windowState();

   	    /**
   	     * State accessor for per-key global state.
   	     */
   	    public abstract KeyedStateStore globalState();
   	}

}
```

其中，参数`key`是指在`keyBY()`方法中指定的`KeySelector`对象。在元组索引键或字符串字段引用的情况下，此键类型始终为元组，你必须手动将其转换为正确大小的元组才能提取键字段。

以下代码段展示了`ProcessWindowFunction`如何计算窗口中元素的数量，除此之外，在输出中还添加了窗口的一些额外信息。

```java
DataStream<Tuple2<String, Long>> input = ...;

input
  .keyBy(t -> t.f0)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .process(new MyProcessWindowFunction());

/* ... */

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

  @Override
  public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
    long count = 0;
    for (Tuple2<String, Long> in: input) {
      count++;
    }
    out.collect("Window: " + context.window() + "count: " + count);
  }
}
```

### ProcessWindowFunction with Incremental Aggregation

使用`ProcessWindowFunction`进行类似于count的聚合是很低效的。如果把`ReduceFunction、AggregateFunction`和`ProcessWindowFunction`进行组合，这样既可以做到元素的增量聚合，又可以在`ProcessWindowFunction`中获取更多元数据信息。

另外，你也可以使用传统的`WindowFunction`进行增量窗口聚合。

#### Incremental Window Aggregation with ReduceFunction

以下示例展示了如何将增量ReduceFunction与ProcessWindowFunction组合，将ReduceFunction的输出作为ProcessWindowFunction的输入，以返回窗口中最小的事件以及窗口的开始时间。

```java
DataStream<SensorReading> input = ...;

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .reduce(new MyReduceFunction(), new MyProcessWindowFunction());

// Function definitions

private static class MyReduceFunction implements ReduceFunction<SensorReading> {

  public SensorReading reduce(SensorReading r1, SensorReading r2) {
      return r1.value() > r2.value() ? r2 : r1;
  }
}

private static class MyProcessWindowFunction
    extends ProcessWindowFunction<SensorReading, Tuple2<Long, SensorReading>, String, TimeWindow> {

  public void process(String key,
                    Context context,
                    Iterable<SensorReading> minReadings,
                    Collector<Tuple2<Long, SensorReading>> out) {
      SensorReading min = minReadings.iterator().next();
      out.collect(new Tuple2<Long, SensorReading>(context.window().getStart(), min));
  }
}
```

#### Incremental Window Aggregation with AggregateFunction

以下示例显示了如何将增量AggregateFunction与ProcessWindowFunction组合，将AggregateFunction的输出作为ProcessWindowFunction的输入，以计算平均值，并将key和窗口与平均值一起发出。

```java
DataStream<Tuple2<String, Long>> input = ...;

input
  .keyBy(<key selector>)
  .window(<window assigner>)
    // AggregateFunction的输出作为ProcessWindowFunction的输入
  .aggregate(new AverageAggregate(), new MyProcessWindowFunction());

// Function definitions

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
private static class AverageAggregate
    implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
  @Override
  public Tuple2<Long, Long> createAccumulator() {
    return new Tuple2<>(0L, 0L);
  }

  @Override
  public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
    return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
  }

  @Override
  public Double getResult(Tuple2<Long, Long> accumulator) {
    return ((double) accumulator.f0) / accumulator.f1;
  }

  @Override
  public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
  }
}

private static class MyProcessWindowFunction
    extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

  public void process(String key,
                    Context context,
                    Iterable<Double> averages,
                    Collector<Tuple2<String, Double>> out) {
      Double average = averages.iterator().next();
      out.collect(new Tuple2<>(key, average));
  }
}
```

### Using per-window state in ProcessWindowFunction

对于`ProcessWindowFunction`，除了像*rich function*一样可以访问keyed state，还可以访问窗口函数当前处理的窗口的keyed state，这是两种不同的状态，后者就是*per-window state*，这两种窗口状态分别对应两种不同的逻辑窗口概念：

* 通过`window()`算子定义的窗口，可以是大小为1小时的滚动窗口，也可以是大小为2小时、步长为1小时的滑动窗口。
* 给定key的已定义窗口的具体实例（`Window`对象）。比如说，对于user-id xyz，这可能是从12:00到13:00的时间窗口。这是基于窗口定义的，根据作业当前正在处理的key的数量以及事件所处的时间段，将有许多窗口。

per-window state就是以上的第二种窗口的状态。这意味着，如果我们处理的事件中有1000个不同的key，并且当前所处理的元素全部在[12:00, 13:00)这个时间窗口内，那么在该时间内将会有1000个窗口实例，每个窗口实例都拥有自己的per-window state（这种窗口实例既是*per-key*，又是*per-window*，即每个key的每个逻辑窗口都对应一个窗口实例。）。

`ProcessWindowFunction`抽象类的`process()`方法中的`Context`参数，提供了两个方法用来获取这两种类型的state。

* `globalState()`，访问*per-key global state*。
* `windowState()`，访问*per-key and per-window state*，在此简称为*per-window state*。

如果你预测到某个窗口可能会多次触发，那么per-window state的特性就很有用，比如你对迟到的数据进行了迟到的窗口触发，或者当你有一个进行推测性早期触发的自定义触发器时，就可能造成窗口的多次触发。这种情况下，你可以在per-window state中存储上一次触发的信息或者触发的次数。

正如`ProcessWindowFunction`源码中注释所说，若你是用了*per-window state*，需要实现`ProcessWindowFunction`抽象类中的`clear()`抽象方法，以便窗口在被清理掉时，可以清楚窗口的状态。

> `ProcessWindowFunction`中的状态分析。
>
> * `ProcessWindowFunction`是一个抽象类，定义如下：
>
>     ```java
>     abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window]
>         extends AbstractRichFunction {...}
>     ```
>
>     可以发现它是继承自`AbstractRichFunction`抽象类，该抽象类是*rich*的，所以其具有`getRuntimeContext() `方法，可以来获取状态：`getRuntimeContext().getState()`，这是一种状态。
>
> * 而在`ProcessWindowFunction`中的`process()`方法中，还有个`Context`类型的参数，`Context`类是`ProcessWindowFunction`的内部类，其中有两个方法可以用来访问状态：`public abstract KeyedStateStore windowState();`和`public abstract KeyedStateStore globalState();`
>
> 对于以上三种访问状态的方式，其中`getRuntimeContext().getState()`与`ProcessWindowFunction.Context`中的`globalState()`是等价的，他俩返回的都是全局状态（global state，Flink源码中也叫做per-key state），该状态在具有相同key的所有窗口之间共享。而`ProcessWindowFunction.Context`中的`windowState()`返回的是per-window状态，该状态根据key进行隔离，即使是相同的key。
>

### WindowFunction (Legacy)

`WindowFunction`通过`apply()`方法来指定，是`ProcessWindowFunction`的老版本，用`ProcessWindowFunction`的地方可以用`WindowFunction`替换。`WindowFunction`提供了较少的上下文元数据信息，并且不支持某些高级特性，比如per-window keyed state。该接口在未来的版本中将废弃。

其源码如下代码段：

```java
public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param key The key for which this window is evaluated.
   * @param window The window that is being evaluated.
   * @param input The elements in the window being evaluated.
   * @param out A collector for emitting elements.
   *
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception;
}
```

示例代码：
```java
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .apply(new MyWindowFunction());
```

## Triggers

窗口触发器（Triggers）通过返回一个`TriggerResult`枚举对象，来决定窗口接下来的行为或动作，比如，触发窗口计算、清理并丢弃窗口。每个窗口分配器都有一个默认的触发器，如果默认的触发器不满足需求，也可以使用`trigger(...)`方法指定自定义的触发器。

`Trigger`抽象类有五个方法，允许触发器对不同的事件作出反应：

* `onElement()`，当每个元素被分配到窗口中时，该方法都会被调用一次。例如在`EventTimeTrigger`触发器中，该方法会判断若是watermark超过了当前窗口的结束时间，则返回`TriggerResult.FIRE`，否则返回`TriggerResult.CONTINUE`。
* `onEventTime()`，当基于event-time的定时器触发时，会调用该方法。
* `onProcessingTime()`，当基于processing-time的定时器触发时，会调用该方法。
* `onMerge()`，与有状态触发器（stateful triggers）相关，在两个触发器对应的窗口合并时，合并它们的触发器的状态（trigger state）。但是在该方法执行之前，会先调用`Trigger`中的`canMerge()`方法以确定该触发器是否支持触发器状态的合并，比如会话窗口（sessinon window）在窗口合并时，其触发器状态就是支持合并的。
* `clear()`，当该触发器对应的窗口被移除时，会调用该方法，用以清除该触发器持有的状态数据。

以上方法有两点需要注意：

1. 前三个方法会返回一个`TriggerResult`枚举对象，该对象决定了窗口将会发生什么，例如是应该调用窗口函数，还是应该清除窗口中的元素、并丢弃该窗口。对窗口有以下几种处理方式（也是`TriggerResult`枚举的成员）：
    * `CONTINUE`，什么都不做
    * `FIRE`，触发窗口计算（窗口函数），并发送出计算结果。但是该窗口不会被清除，其中的元素将被保留。
    * `PURGE`，清理掉窗口内所有元素，并丢失该窗口，不会触发窗口函数，也不会发出任何元素。
    * `FIRE_AND_PURGE`，触发窗口函数，然后清理掉窗口中所有元素。
2. 这三个方法都可以用于注册基于processing-time和基于event-time的定时器（`Timer`）。

### Fire and Purge

一旦触发器确定窗口已经准备好进行处理，则触发器就会进行触发。触发器触发后，将返回一个`TriggerResult`枚举对象，如前文所述。这是窗口算子（window operator）发出当前窗口结果的信号。对于`ProcessWindowFunction`，所有窗口中的元素都要经过该窗口函数进行处理（可能是在将它们传递给驱逐器之后）；对于`ReduceFunction`和`AggregateFunction`只是将其之前的聚合结果发送出去（因为这俩是增量聚合）。

若触发器触发时返回的是`FIRE`枚举对象，则窗口在应用窗口函数后，将保留窗口中的元素；若触发器触发时返回的是`FIRE_AND_PURGE`枚举对象，则窗口在应用窗口函数后，将清除窗口中的元素。默认情况下，Flink内置的窗口触发器在触发时，都会返回`FIRE`枚举对象，只触发窗口计算，不会清理窗口中的任何数据（因为Allowed Lateness可能会使得窗口触发器再次触发，只有在Allowed Lateness过期只后才可以彻底清理掉窗口）。

> 清除（Purging）只是移除窗口中的内容，但是关于窗口和触发器状态的潜在元数据信息将会保留。

### Default Triggers of WindowAssigners

窗口分配器（`WindowAssigner`）中默认的窗口触发器（`Trigger`）可以适用于很多的场景。

所有的基于event-time的窗口分配器都将`EventTimeTrigger`作为默认的触发器，一旦水位线（watermark）超过了窗口的结束时间，触发器就会触发。

`GlobalWindows`分配器的默认触发器是`NeverTrigger`，这意味着永远不会触发。因此，当你使用`GlobalWindows`分配器时，你可以使用自定义的触发器。

通过`trigger(...)`方法指定触发器后，将会覆盖`WindowAssigner`中默认的触发器。例如，你为`TumblingEventTimeWindow`指定了`CountTrigger`触发器，窗口的触发将基于count，而不再基于时间进度（progress of time）。如果你既想基于时间进度进行触发，又想基于count进行触发，则你需要实现自定的触发器。

### Built-in and Custom Triggers

Flink内置了几个触发器：

* `EventTimeTrigger`，基于event-time的处理进度进行触发，其触发衡量标准是watermark是否超过了窗口的结束时间。
* `ProcessingTimeTrigger`，基于processing-time，其他与上述类似。
* `CountTrigger`，当窗口中的元素超过给定的上限后，就会触发。
* `PurgingTrigger`，接受另一个`Trigger`类型的参数，并将其转换为*purge*类型。

如果你需要实现一个自定义触发器，你可以通过继承抽象类`Trigger`实现。请注意，API仍在发展中，可能会在Flink的未来版本中发生变化。

## Evictors

除了窗口分配器、窗口触发器、窗口函数外，Flink的window模型还还可指定可选的驱逐器（Evictor），可以通过`evictor(...)`方法指定。驱逐器主要用来遍历窗口中的元素列表，并决定哪些元素需要移除、哪些元素需要保留，若没有驱逐器，所有元素都将保留。驱逐器可以在窗口触发器触发后、窗口函数执行之前或之后清理掉窗口中的元素。`Evictor`接口有两个方法：

```java
/**
 * Optionally evicts elements. Called before windowing function.
 *
 * @param elements The elements currently in the pane.
 * @param size The current number of elements in the pane.
 * @param window The {@link Window}
 * @param evictorContext The context for the Evictor
 */
void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

/**
 * Optionally evicts elements. Called after windowing function.
 *
 * @param elements The elements currently in the pane.
 * @param size The current number of elements in the pane.
 * @param window The {@link Window}
 * @param evictorContext The context for the Evictor
 */
void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
```

`evictBefore()`方法用在窗口函数执行之前，`evictAfter()`用在窗口函数执行之后。如果在窗口函数执行之前驱逐元素，则窗口函数不会处理这些被驱逐的元素。

Flink中的内置驱逐器：

* `CountEvictor`，以元素计数为标准，决定元素是否删除。最多保留窗口中用户指定数量的元素，并丢弃窗口缓冲区开头的剩余元素。源码如下：

    ```java
        private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
            if (size <= maxCount) {
                return;
            } else {
                int evictedCount = 0;
                for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
                        iterator.hasNext(); ) {
                    iterator.next();
                    evictedCount++;
                    if (evictedCount > size - maxCount) {
                        break;
                    } else {
                        iterator.remove();
                    }
                }
            }
        }
    ```

* `DeltaEvictor`，需要传入`DeltaFunction`和`double threshold`（一个计算函数，一个阈值），然后遍历每个元素，计算每个元素与列表中最后一个元素的delta值，若该值大于等于传入的threshold，则移除该元素。源码如下：

    ```java
        private void evict(Iterable<TimestampedValue<T>> elements, int size, EvictorContext ctx) {
            TimestampedValue<T> lastElement = Iterables.getLast(elements);
            for (Iterator<TimestampedValue<T>> iterator = elements.iterator(); iterator.hasNext(); ) {
                TimestampedValue<T> element = iterator.next();
                if (deltaFunction.getDelta(element.getValue(), lastElement.getValue())
                        >= this.threshold) {
                    iterator.remove();
                }
            }
        }
    ```

    

* `TimeEvictor`，以时间为判断标准，决定元素是否删除。其接受一个windowSize的参数，然后获取当前窗口中所有元素的最大时间戳max_ts（类似于`TimeWindow`中的结束时间），然后max_ts减去windowsize，得到min_ts（类似于`TimeWindow`的开始时间），然后遍历窗口中的所有元素，不在min_ts和max_ts之前的元素全部移除。源码如下：

    ```java
        private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
            if (!hasTimestamp(elements)) {
                return;
            }
    
            long currentTime = getMaxTimestamp(elements);
            long evictCutoff = currentTime - windowSize;
    
            for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
                    iterator.hasNext(); ) {
                TimestampedValue<Object> record = iterator.next();
                if (record.getTimestamp() <= evictCutoff) {
                    iterator.remove();
                }
            }
        }
    ```

默认情况下，如果我们不指定在窗口函数之前还是在窗口函数之后，所有的内置驱逐器都会在应用窗口函数之前进行应用。看源码验证：
```java
// 以CountEvictor为例
    public static <W extends Window> CountEvictor<W> of(long maxCount) {
        return new CountEvictor<>(maxCount);
    }

    private CountEvictor(long count) {
        this.maxCount = count;
        // 默认在窗口函数之前
        this.doEvictAfter = false;
    }
```

关于驱逐器有两点需要注意：

1. 指定驱逐器可以防止预聚合（pre-aggregation），因为窗口中的所有元素在应用窗口函数之前要先经过驱逐器。这意味着拥有驱逐器的窗口将创建更多的状态。

2. Flink不保证窗口中元素的顺序，这意味着，尽管驱逐器可以从窗口的开头移除元素，但这些元素不一定是最先到达或最后到达的元素。

## Allowed Lateness

当使用event-time窗口时，元素有可能会延迟到达，即Flink用于跟踪事件时间进度的watermark已经超过元素所属窗口的结束时间戳。

默认情况下，当watermark超过窗口结束时间时，迟到的元素将会被删掉，但是Flink允许为窗口算子指定允许的最大迟到时间（Allowed Lateness），Allowed Lateness指定了元素在被丢弃之前允许的迟到的时间，其默认值为0，在这个时间点之前到达的元素仍会被添加到其所属的窗口中。对于有些触发器来说，Allowed Lateness会导致那些迟到但未删除的元素可能会使得窗口被再次触发，比如`EventTimeTrigger`触发器。

为了实现Allowed Lateness机制，Flink需要一直保存窗口的状态（哪怕触发器已经触发），直到Allowed Lateness也已经过期。一旦Allowed Lateness过期，Flink将会彻底移除窗口实例及其状态，正如Window Lifecycle小节中所说。

> 之前在了解完窗口的四个基本组件后，发现窗口可能在经过四个组件后仍未被彻底清理，原来窗口的彻底清理是在**Allowed Lateness过期之后**。

```java
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .<windowed transformation>(<window function>);
```

当使用`GlobalWindows`窗口分配器时，必然要将元素分配到`GlobalWindow`类型的窗口，而因为这种窗口的结束时间为`Long.MAX_VALUE`，所以所有的元素都不会迟到。

### Getting late data as a side output

使用Flink中的旁路输出（side output）特性，我们可以获得迟到的被丢弃数据的数据流。代码示例如下：

```java
final OutputTag<T> lateOutputTag = new OutputTag<T>("late-data"){};

DataStream<T> input = ...;

SingleOutputStreamOperator<T> result = input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .sideOutputLateData(lateOutputTag)
    .<windowed transformation>(<window function>);

DataStream<T> lateStream = result.getSideOutput(lateOutputTag);
```

### Late elements considerations

当指定Allowed Lateness大于0时，在watermark超过窗口结束时间后，该窗口实例及其内容都会继续保存。这时，当一个迟到但没删除的元素到达时，可能导致窗口的再次触发，这些迟到的触发称为`late firing`，因为他们是被迟到的时间触发的，与其第一次触发形成对比。对于会话窗口来说，延迟触发可能会进一步导致窗口合并，因为迟到的事件可能正好位于两个之前存在的、还未合并的窗口之间的`gap`之中，使其gap小于指定的最大gap值，就会导致这两个窗口的merge。

由late firing发出的结果应该作为之前结果的更新。意思是说，如果你的数据流包含了同一个窗口函数的多次计算的结果，在你的应用程序中你需要将这些重复的结果考虑在内，或者是对他们进行去重。

## Working with window results

开窗操作的输出结果仍然是一个`DataStream`。关于开窗操作的相关信息不会保存在结果数据流的元素中，所以如果你想保存窗口的元数据信息，你需要在`ProcessWindowFunction`中，将相关信息编码到输出结果的元素中。但是有个例外，结果元素中唯一的窗口信息是元素的时间戳，该时间戳被设置为该窗口的允许处理的最大时间戳，即`TimeWindow.getEnd()-1`，因为`TimeWindow.getEnd()`时间戳会被排除在窗口可处理的时间戳之外。需要注意的是，这对于event-time窗口和processing-time窗口都生效。即，在经过开窗操作后，所有的输出元素都将有一个时间戳，可能是event-time时间戳，也可能是processing-time时间戳。对于processing-time窗口来说这没什么特别的，但是对于event-time窗口来说，加上水印与窗口的交互方式，可以实现具有相同窗口大小的连续窗口操作。

### Interaction of watermarks and windows

当watermark到达窗口算子时，将会触发两个动作：

* 若窗口的最大时间戳（`TimeWindow.getEnd()-1`）小于该watermark，则该watermark会触发窗口计算。
* watermark按照原样转发给下游算子。

Intuitively, a watermark “flushes” out any windows that would be considered late in downstream operations once they receive that watermark.

### Consecutive windowed operations

如前面所说，开窗结果时间戳的计算方式以及watermark与窗口的交互方式允许将连续的开窗操作串在一起。这在以下场景中很有用：当你想要执行两个连续的开窗操作，并且这两个开窗要使用不同的key，而且还希望来自同一上游窗口的元素最终会出现在同一下游窗口中。如下示例所示：

```java
DataStream<Integer> input = ...;

DataStream<Integer> resultsPerKey = input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(new Summer());

DataStream<Integer> globalResults = resultsPerKey
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    .process(new TopKWindowFunction());
```

上述示例中，第一次操作的时间窗口[0，5）的结果也将在随后的窗口操作中的时间窗口[0，5）中结束。这可以在第一个窗口操作中计算每个key的sum值，并在第二个窗口操作中计算同一窗口内的top k个元素。

## Useful state size considerations

窗口可以定义为很长的时间段，例如一天、一周、甚至一个月，因此会累积很大的状态。关于窗口计算中的状态你需要清楚以下几点：

1. Flink为每个元素所属的窗口创建一个副本。基于此可知，滚动窗口会保留每个元素的一个副本，因为滚动窗口不会重叠，所以一个元素只属于一个窗口。除非它被延迟删除。相反，滑动窗口可能会保存元素的多个副本。因此，窗口大小为1天步长为1秒的滑动窗口可不是个好主意。
2. `ReduceFunction`和`AggregateFunction`可以有效的减少状态存储的需求，因为它们是增量聚合，每个窗口只需要保存一个元素（即聚合中间结果）。相反，`ProcessWindowFunction`需要在状态中保存所有的元素。
3. 若使用了驱逐器（`Evictor`），将会阻止任何的预聚合，因为窗口中的元素在计算之前，必须先在驱逐器中遍历。

