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

滑动窗口分配器（sliding windows assigner）将元素分配到固定大小的窗口内。类似于滚动窗口，可以配置窗口的大小。滑动窗口另一个参数是窗口滑动步长（window slide），用于控制滑动窗口启动的频率。因此，如果滑动步长小于窗口大小，则滑动窗口就会重叠，这时元素就会分配给多个窗口。

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

全局窗口分配器（global windows assigner）将具有相同key的所有元素分配到同一个全局窗口内。因为全局窗口没有结束时间，只能通过自定义窗口触发器触发计算，所以必须为全局窗口指定自定义窗口触发器。

![global windows](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/non-windowed-5978343.svg)

```java
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>);
```

## Window Functions

在定义了窗口分配器（window assigner）后，我们需要指定要在每个窗口上执行的计算。这就是窗口函数（window function）的职责，一旦系统确定窗口已经准备好进行计算，该喊出就应用在窗口中的每个元素上（其中，触发器决定了一个窗口何时准备好）。

窗口函数可以是`ReduceFunction、AggregateFunction、ProcessWindowFunction`。前两个执行效率更高，因为Flink可以对窗口内每个到达元素进行增量聚合。而`ProcessWindowFunction`会获取每个窗口内所有元素的迭代器（会对窗口内所有元素进行一次全量计算）以及元素所属窗口的额外元数据信息。

使用`ProcessWindowFunction`的窗口算子不能像其他窗口函数那样高效地执行，因为Flink在调用窗口函数之前必须在内部缓冲窗口内的所有元素。而如果我们将`ProcessWindowFunction`与`ReduceFuncton、AggregateFunction`进行组合，我们既可以获得窗口元素的增量聚合能力，又可以得到`ProcessWindowFunction`的窗口元数据信息。

### ReduceFunction

`ReduceFunction`定义了两个输入元素进行组合，并生成一个具有相同类型的输出元素。Flink通过`ReduceFunction`实现了窗口内元素的增量聚合。

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

以上的程序示例对窗口内元素的第二个字段进行累加。`ReduceFunction`的底层实现，实际上是将中间“合并的结果”作为状态保存起来，之后每来一个新的数据，就和之前的聚合状态进一步做归约。 

### AggregateFunction

`AggregateFunction`是`ReduceFunction`的通用版本，它有三种参数类型：一个输入类型（IN）、一个累加器类型（ accumulator type，ACC）、以及一个输出类型（OUT）。输入类型是输入流中元素的类型，AggregateFunction具有将一个输入元素添加到累加器的方法。该接口还有另外几个方法：创建初始累加器的方法、将两个累加器合并为一个累加器的方法、从累加器中抽取输出（OUT）的方法。

与`ReduceFunction`一样，Flink也是对窗口中的元素进行增量聚合。

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

`ProcessWindowFunction`中可以获取包含窗口中所有元素的迭代器，以及一个`Context`对象，该对象可以用来访问时间和状态信息，这使得`ProcessWindowFunction`比其他窗口函数更加灵活。这是以性能和资源消耗为代价的，因为窗口中的元素不能增量聚合，反而需要进行缓存，直到窗口触发器出发窗口函数的计算。

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

注意，使用`ProcessWindowFunction`进行类似于count的聚合是很低效的。下一小节将对`ReduceFunction、AggregateFunction`和`ProcessWindowFunction`进行组合，这样既可以做到元素的增量聚合，又可以在`ProcessWindowFunction`中获取更多元数据信息。

### ProcessWindowFunction with Incremental Aggregation

使用`ProcessWindowFunction`进行类似于count的聚合是很低效的。如果把`ReduceFunction、AggregateFunction`和`ProcessWindowFunction`进行组合，这样既可以做到元素的增量聚合，又可以在`ProcessWindowFunction`中获取更多元数据信息。

另外，你也可以使用传统的`WindowFunction`进行增量窗口聚合。

#### Incremental Window Aggregation with ReduceFunction

以下示例展示了如何将增量ReduceFunction与ProcessWindowFunction组合，以返回窗口中最小的事件以及窗口的开始时间。

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

以下示例显示了如何将增量AggregateFunction与ProcessWindowFunction组合以计算平均值，并将键和窗口与平均值一起发出。

```java
DataStream<Tuple2<String, Long>> input = ...;

input
  .keyBy(<key selector>)
  .window(<window assigner>)
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

对于`ProcessWindowFunction`，除了像*rich function*一样可以访问keyed state，还可以访问窗口函数当前处理的窗口的keyed state，这是两种不同的状态，后者就是*per-window state*。这里涉及到不同的两种窗口：

* 通过`window()`算子定义的窗口，可以是大小为1小时的滚动窗口，也可以是大小为2小时、步长为1小时的滑动窗口。
* 给定key的已定义窗口的具体实例。比如说，对于user-id xyz，这可能是从12:00到13:00的时间窗口。这是基于窗口定义的，根据作业当前正在处理的key的数量以及事件所处的时间段，将有许多窗口。

per-window state就是以上的第二种窗口的状态。这意味着，如果我们处理的事件中有1000个不同的key，并且当前所处理的元素全部在[12:00, 13:00)这个时间窗口内，那么在该时间内将会有1000个窗口实例，每个窗口实例都拥有自己的per-window state。

`ProcessWindowFunction`抽象类的`process()`方法中的`Context`参数，提供了两个方法用来获取这两种类型的state。

* `globalState()`，用来访问不具体属于某个窗口的状态。
* `windowState()`，用来访问具体属于某个窗口的状态。

如果你预测到某个窗口可能会多次触发，那么per-window state的特性就很有用，比如你对迟到的数据进行了迟到的窗口触发，或者当你有一个进行推测性早期触发的自定义触发器时，就可能造成窗口的多次触发。这种情况下，你可以在per-window state中存储上一次触发的信息或者触发的次数。

当使用windowed state时，当窗口被清理掉时，也要清理掉窗口的状态，可以通过`clear()`方法实现。

### WindowFunction (Legacy)

`WindowFunction`是`ProcessWindowFunction`的老版本，用`ProcessWindowFunction`的地方可以用`WindowFunction`替换。`WindowFunction`提供了较少的上下文元数据信息，并且不支持某些高级特性，比如per-window keyed state。该接口在未来的版本中将废弃。

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

























