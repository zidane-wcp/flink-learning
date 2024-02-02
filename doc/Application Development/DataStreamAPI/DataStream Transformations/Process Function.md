

> 本章节部分内容参考自：https://blog.csdn.net/zqf787351070/article/details/129102004

## Overview

为了使代码具有更强大的表现力、易用性及灵活性，Flink本身提供了多层API供用户选择，如下图所示。其中，Flink中支持的所有算子都是在`DataStream/DataSet API`层实现的，例如`map、filter`等。

![Programming levels of abstraction](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/levels_of_abstraction.svg)

而在更底层，Flink允许用户不需要定义具体的算子，而是提炼出一个统一的处理操作（Process Function），它是所有转换算子的一个概括性表达，它不限定具体做什么，而是可以由用户自定义各种灵活的处理逻辑。

Process Function作为一种底层的流处理操作，可以访问流处理应用中所有的基本构建块（非循环），基于此，Process Function可以说能实现对数据流的任何操作。：

* 事件（即数据流中的元素）。
* 状态（容错，一致性，只能应用在keyed stream上）。
* 定时器（即timer，可以基于event-time和processing-time，只能应用在keyed stream上）。

Process Function可以被认为是一个能够访问keyed state和定时器的`FlatMapFunction`，他会被数据流中的每个元素进行调用并处理。

对于容错状态，Process Function允许访问Flink的keyed state，可以通过`RuntimeContext`对象来访问，类似于其他有状态函数访问keyed state的方式。

定时器允许应用程序对event time和processing time的变化作出反应。每次调用`ProcessFunction.processElement(...)`方法时，方法中都会传入一个`Context`对象，该对象可以用来访问元素的event time时间戳和*TimerService*。该`TimerService`可以用于为未来的某个基于event time或基于processing time的时刻注册回调，回调就是调用`onTimer()`方法。对于event-time定时器，若当前的watermark超过了定时器的时间戳，则会调用`onTimer()`方法；而对于processing-time定时器，当机器时钟到达指定的时间时，就会调用`onTimer()`方法。在`onTimer()`调用过程中，所有状态的作用域再次限定为创建计时器时使用的键，从而允许计时器操作keyed state。

```java
stream.keyBy(...).process(new MyProcessFunction());
```

## The Process Function

所有的Process Function都是继承自`AbstractRichFunction`，所以也都拥有*Rich Function*的能力。

Flink提供了以下不同的process function：

* `ProcessFunction`：最基本的处理函数，基于`DataStream`调用`process()`并将该处理函数作为参数传入。`ProcessFunction`也可以应用在`KeyedStream`上，`KeyedStream`是`DataStream`的子类，但是Flink源码中已经标记为`@Deprecated`。

    ```java
    datastream.process(new ProcessFunction<>());
    ```
* `KeyedProcessFunction`：对keyBy分区之后的流的处理函数，基于`KeyedStream`调用`process()`并将该处理函数作为参数传入。`KeyedProcessFunction`可以说是`ProcessFunction`的扩展，除了`ProcessFunction`的能力之外，还可以在其内部类`Context`和`OnTimerContext`中通过`public abstract K getCurrentKey()`方法访问当前的key。

    ```java
    datastream.keyBy().process(new KeyedProcessFunction<>())
    ```
* `ProcessWIndowFunction`：开窗操作之后的处理函数，基于`WindowStream`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream.keyBy().window().process(new ProcessWIndowFunction<>())
    ```
* `ProcessAllWindowFunction`：开窗操作（`datastream.windowAll()`）之后的处理函数，基于`AllWindowedStream`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream.windowAll().process(new ProcessAllWindowFunction<>())
    ```
* `CoProcessFunction`：合并两条流之后的处理函数，基于`ConnectedStreams`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream1.connect(datastream2).process(new CoProcessFunction<>())
    ```

* `KeyedCoProcessFunction`：对`ConnectedStreams`进行keyBy分区之后的处理函数，基于`ConnectedStreams`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream1.connect(datastream2).keyBy().process(new KeyedCoProcessFunction<>())
    ```
* `ProcessJoinFunction`：区间关联两条流之后的处理函数，基于`IntervalJoined`调用`process()`并将该处理函数作为参数传入。注意，`IntervalJoined`是`KeyedStream`的内部类。

    ```java
    DataStream<Integer> datastream1 = ...;
    DataStream<Integer> datastream2 = ...;
    
    datastream1
        .keyBy(<KeySelector>)
        .intervalJoin(datastream2.keyBy(<KeySelector>))
        .between(Time.milliseconds(-2), Time.milliseconds(1))
        .process (new ProcessJoinFunction<Integer, Integer, String(){
    
            @Override
            public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
                out.collect(left + "," + right);
            }
        });
    ```
    
* `BroadcastProcessFunction`：广播连接流处理函数，基于`BroadcastConnectedStream`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream.connect(broadcaststream).process(new BroadcastProcessFunction<>())
    ```
* `KeyedBroadcastProcessFunction`：基于keyby分区的广播连接流的处理函数，基于`BroadcastConnectedStream`调用`process()`并将该处理函数作为参数传入。

    ```java
    datastream.keyBy().connect(broadcaststream).process(new BroadcastProcessFunction<>())
    ```

    

## Low-level Joins

为了实现对两条输入数据流的底层操作，你可以使用`CoProcessFunction`或`KeyedCoProcessFunction`，这两个function都是用来处理两条不同的数据流的，而其中的`processElement1(...)`和`processElement2(...)`方法可以独立的处理两条流中的元素。

可以通过以下基本步骤来实现底层join：

* 为一个（或两个）流创建状态。
* 当接收到该条流的数据时，更新状态。
* 当接收到另一条流的数据时，访问之前定义的状态，并生成join结果。

例如，你可以将客户的信息数据保存在状态中，当交易数据流的元素到来时，你可以将状态中保存的客户数据丰富到交易数据中。如果你想对于无序的事件仍能计算出完整且具有确定性的关联结果，那么当客户数据的watermark超过交易时间时，你可以使用定时器评估并发送出交易数据的关联结果。

## Example

以下例子中，`KeyedProcessFunction`维护每个key的计数，每分钟输出一个key/count，而不更新这个key：

* count、key、上次更新时间，这三个信息保存在`ValueState`对象中，由key确定其作用域。
* 对于每个元素，`KeyedProcessFunction`对其计数器加1，并更新时间戳。
* `KeyedProcessFunction`对于每个到来的元素，将回调安排在一分钟之后（基于event time，注册定时器）。
* 对于每次回调，在定时器中检查当前回调时间与上次更新时间是否匹配，若匹配则输出key/count对。

> 这个例子原本可以用会话窗口来实现，这里我们用`KeyedProcessFunction`实现是为了展示其提供的基本用法。

```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


// the source data stream
DataStream<Tuple2<String, String>> stream = ...;

// apply the process function onto a keyed stream
DataStream<Tuple2<String, Long>> result = stream
    .keyBy(value -> value.f0)
    .process(new CountWithTimeoutFunction());

/**
 * The data type stored in the state
 */
public class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
public class CountWithTimeoutFunction 
        extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value, 
            Context ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp, 
            OnTimerContext ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
```

## The KeyedProcessFunction

`KeyedProcessFunction`是`ProcessFunction`的扩展，可以在其`onTimer()`方法中访问定时器的key。

```java
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}
```

## Timers

基于processing-time的定时器和基于event-time的定时器都由`TimerService`进行维护，并排队等待执行。

`TimerService`按key和timestamp对定时器进行去重，也就是说对于每个key和每个timestamp最多只有一个定时器，如果对于同一个timestamp注册了多个定时器，`onTimer()`方法只会被执行一次。

Flink对`onTimer()`和`processElement()`方法的执行是同步的，所以用户不用担心对状态的并发更新问题。

### Fault Tolerance

定时器是支持容错的，它会与应用程序的state一起进行checkpoint，如果发生故障或从savepoint启动作业时，定时器也会被恢复。

定时器的checkpoint操作总是异步执行的，除了除了RocksDB后端、与增量快照、与基于堆的计时器的组合。要注意如果定时器的数量很多，也会增加checkpoint的时间，因为定时器是checkpoint状态的一部分。

### Timer Coalescing

Flink对于每个key和timestamp只维护一个定时器，所以你可以降低定时器的执行频率来合并他们，这样就能减少定时器的数量。

对于1秒（事件或处理时间）的计时器分辨率，可以将目标时间四舍五入到整秒。计时器最多会提前1秒发射，但不会晚于要求的毫秒精度。因此，每个键和秒最多有一个计时器。

```java
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
```

由于event-time定时器只在watermark进入时触发，你也可以使用当前watermark来安排这些计时器并将其与下一个水印合并：

```java
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
```

定时器也可以被停止和删除：

```java
// processing-time
long timestampOfTimerToStop = ...;
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);

// event-time
long timestampOfTimerToStop = ...;
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
```

