## Overview

算子（operator）将一个或多个数据流转换为新的数据流。程序可以将多种转换（transformation）组合成复杂的数据流拓扑。

> Flink中的算子（operator）和转换（transformation）是两个相似的概念，可以互换使用，反映到代码层面，就是各种Function。

## Basic DataStream Transformations

本节介绍Flink支持的一些基本的算子，每个算子的介绍包括三部分内容：

* 输入的数据流类型和输出的数据流类型。
* 基本功能介绍。
* 示例代码。

### Map

**DataStream → DataStream**

获取一个元素，并生成一个元素。输入与输出是一对一的关系。

```java
DataStream<Integer> dataStream = //...
dataStream.map(new MapFunction<Integer, Integer>() {
    @Override
    public Integer map(Integer value) throws Exception {
        return 2 * value;
    }
});
```

### FlatMap

**DataStream → DataStream**

获取一个元素，并生成零个或多个元素。

```java
dataStream.flatMap(new FlatMapFunction<String, String>() {
    @Override
    public void flatMap(String value, Collector<String> out)
        throws Exception {
        for(String word: value.split(" ")){
            out.collect(word);
        }
    }
});
```

### Filter

**DataStream → DataStream**

为每个元素计算一个布尔函数，并保留那些函数返回true的元素。

```java
dataStream.filter(new FilterFunction<Integer>() {
    @Override
    public boolean filter(Integer value) throws Exception {
        return value != 0;
    }
});
```

### KeyBy

**DataStream → KeyedStream**

从逻辑上将流划分为不相交的分区。具有相同key的所有记录都被分配到相同的分区。在内部，`keyby()`是用hash分区实现的。有不同的方法可以指定key。

```java
dataStream.keyBy(value -> value.getSomeKey());
dataStream.keyBy(value -> value.f0);
```

> 注意，以下类型不能作为key：
>
> 1. 某个POJO类型，并且没有重写（override）`hashCode()`方法，而是依赖`Object.hashCode()`的实现。
> 2. 任意类型的数组。

### Reduce

**KeyedStream → DataStream**

Reduce算子，就是对KeyedStream进行滚动reduce的操作，它会将上一个reduce过的值与当前的element结合，产生新的值并发送出去。比如Flink示例程序*WordCount*中用的sum操作，就是对Reduce算子的封装。

```java
keyedStream.reduce(new ReduceFunction<Integer>() {
    @Override
    public Integer reduce(Integer value1, Integer value2)
    throws Exception {
        return value1 + value2;
    }
});

// 另一个WordCount的示例
dataStream.keyby("word")
    .sum("count");
// 也可以调用更底层的reduce算子，sum是对reduce的封装。
dataStream.keyby("word")
    .reduce(new ReduceFunction<WordWithCount>(){
        @Override
        public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
            return new WordWithCount(a,word, a.count + b.count)
        }
    });
```

### Window

**KeyedStream → WindowedStream**

可以在已经分区（keyby）的 KeyedStream 上定义 Windows。Window 根据某种规则（例如，最近 5 秒内到达的数据）对每个key的数据进行分组。

关于Window的内容较多，在《Window》章节中进行详细介绍。

```java
dataStream
  .keyBy(value -> value.f0)
  .window(TumblingEventTimeWindows.of(Time.seconds(5))); 
```

### WindowAll

**DataStream → AllWindowedStream**

Window也可以在常规的DataStream流上定义，此时的Window会根据某种规则对数据流中的所有事件（元素）进行分组（比如，最近5秒内到达的数据）。

> 很多情况下，这是一种非并行的转换操作，所有的记录都会被分配到windowAll算子的同一个task中。

```java
dataStream
  .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
```

### Window Apply

**WindowedStream → DataStream**

**AllWindowedStream → DataStream**

对整个窗口应用一个通用的function。如下的function展示了对窗口中的元素进行手动求和的过程。

> 对于WindowedStream，要使用WindowFunction。
>
> 对于AllWindowedStream，要使用AllWindowFunction。

```java
windowedStream.apply(new WindowFunction<Tuple2<String,Integer>, Integer, Tuple, Window>() {
    public void apply (Tuple tuple,
            Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});

// applying an AllWindowFunction on non-keyed window stream
allWindowedStream.apply (new AllWindowFunction<Tuple2<String,Integer>, Integer, Window>() {
    public void apply (Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});
```

### WindowReduce

**WindowedStream → DataStream**

对窗口应用reduce function，并返回reduce后的结果。

```java
windowedStream.reduce (new ReduceFunction<Tuple2<String,Integer>>() {
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        return new Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
    }
});
```

> 疑问，看起来，ReduceFunction既可以应用于keyedStream，也可以应用于windowedStream，是否可以应用于AllWindowedStream？需要测试，找出ReduceFunction都可以应用哪些stream。

### Union

**DataStream* → DataStream**

两个或多个流进行union，创建一个新的流，该流包含所有流的所有元素。如果某个流与其自身进行union，则新流中每个元素包含两次。

```java
dataStream.union(otherStream1, otherStream2, ...);
```

### Window Join

**DataStream,DataStream → DataStream**

根据给定的关联key以及window，两个数据流进行关联（join）。

```java
dataStream.join(otherStream)
    .where(<key selector>).equalTo(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new JoinFunction () {...});
```

### Interval Join

**KeyedStream,KeyedStream → DataStream**

在给定的时间间隔内，使用给定的关联key，关联两个KeyedStream数据流，对于两个数据流中的元素e1和e2，关联条件是这样的：`e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound`。

```java
// this will join the two streams so that
// key1 == key2 && leftTs - 2 < rightTs < leftTs + 2
keyedStream.intervalJoin(otherKeyedStream)
    .between(Time.milliseconds(-2), Time.milliseconds(2)) // lower and upper bound
    .upperBoundExclusive(true) // optional
    .lowerBoundExclusive(true) // optional
    .process(new IntervalJoinFunction() {...});
```

### Window CoGroup

**DataStream,DataStream → DataStream**

在给定的key和window内，对两个数据流进行协同分组（CoGroup）。

```java
dataStream.coGroup(otherStream)
    .where(0).equalTo(1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new CoGroupFunction () {...});
```

### Connect

**DataStream,DataStream → ConnectedStreams**

连接（connect）两个数据流，并保留各自的数据类型。连接允许两个数据流共享状态（state）数据。

```java
DataStream<Integer> someStream = //...
DataStream<String> otherStream = //...

ConnectedStreams<Integer, String> connectedStreams = someStream.connect(otherStream);
```

> 从源码看connect与union的区别：
>
> ```java
> // union
>     public final DataStream<T> union(DataStream<T>... streams)
> // connect
>     public <R> ConnectedStreams<T, R> connect(DataStream<R> dataStream) {
> 
> ```
>
> ```java
> public class ConnectedStreams<IN1, IN2> {
> 
>     protected final StreamExecutionEnvironment environment;
>     protected final DataStream<IN1> inputStream1;
>     protected final DataStream<IN2> inputStream2;
>     ...
> }
> ```
>
> 从源码可以看出，union是真正的合并了两个或多个数据流，返回一个`DataStream<T>`对象，而connect返回的是`ConnectedStreams<IN1, IN2>`对象，该对象对两条流进行了封装，IN1表示左边的流，IN2表示右边的流。

### CoMap, CoFlatMap

**ConnectedStream → DataStream**

与普通的map和flatMap算子类似，只是CoMap和CoFlatMap是作用在ConnectedStream上的。

```java
connectedStreams.map(new CoMapFunction<Integer, String, Boolean>() {
    @Override
    public Boolean map1(Integer value) {
        return true;
    }

    @Override
    public Boolean map2(String value) {
        return false;
    }
});
connectedStreams.flatMap(new CoFlatMapFunction<Integer, String, String>() {

   @Override
   public void flatMap1(Integer value, Collector<String> out) {
       out.collect(value.toString());
   }

   @Override
   public void flatMap2(String value, Collector<String> out) {
       for (String word: value.split(" ")) {
         out.collect(word);
       }
   }
});
```

### Iterate

**DataStream → IterativeStream → ConnectedStream**

在数据流中创建一个反馈（feedback）的循环，就是将某个算子的输出流重定向到之前的算子，以实现数据流的迭代。这对于定义持续更新模型的算法特别有用。以下代码展示了某个数据流连续应用迭代体，大于0的元素被发送回反馈通道进行迭代，其余元素被转发到下游。

```java
IterativeStream<Long> iteration = initialStream.iterate();
DataStream<Long> iterationBody = iteration.map (/*do something*/);
DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value > 0;
    }
});
iteration.closeWith(feedback);
DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value <= 0;
    }
});
```

其他示例的完整实现：

```java
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Long> someIntegers = env.fromSequence(0, 10);
    // 创建迭代数据流
    IterativeStream<Long> iteration = someIntegers.iterate();
    // 对于迭代数据流，需要做一些逻辑，使其最终能够收敛，否则就是无限迭代
    // 对于迭代数据流，经过以下map算子之后，还会进入iteration这个迭代流中，继续进入map算子。
    DataStream<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
        @Override
        public Long map(Long value) throws Exception {
            return value - 1;
        }
    });
    // 以上的在map算子上应用迭代流，就相当于做while循环。
    // 以下对iterationBody应用filter算子，就相当于while()循环中的条件，即当value>0时才会循环，也就是 while(value>0)。
    DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
        @Override
        public boolean filter(Long value) throws Exception {
            return (value > 0);
        }
    });
    // 设置完循环条件后，应用到迭代流上。
    iteration.closeWith(feedback);
    iterationBody.print();
    env.execute();
}
```

## Physical Partitioning

Flink 可以通过以下function对转换后的流进行精确分区提供更底层的控制（如果需要）。

> 分区（Partitioning）操作就是将数据进行重新分布，传递到不同的流分区去进行下一步处理。例如对于keyby操作，就是一种按照key的hash来急性重新分区的操作。只不过这种分区只能保证吧数据按照key分开，至于分的均匀不均匀、每个key的数据具体会分到哪一分区去，这些都无法控制。
>
> 而物理分区（Physical Partitioning）可以做到真正的控制分区策略，精准的调配数据，告诉每个数据应该去哪里。其实这种分区方式在一些情况下已经在发生了：例如我们编写的程序可能对多个处理任务设置了不同的并行度，那么当数据执行的上下游任务并行度变化时，数据就不应该还在当前分区以直通（forward）方式传输了，因为如果并行度变小，当前分区可能没有下游任务了；而如果并行度变大，所有数据还在原先的分区处理就会导致资源的浪费。所以这种情况下，系统会自动的将数据均匀地发往下游所有的并行任务，保证各个分区的负载均衡。
>
> 有些时候，我们还需要手动控制数据分区分配策略。比如当数据发生倾斜的时候，系统无法自动调整，这时就需要我们重新进行负载均衡，将数据流较为平均的发送到下游任务操作分区中去。Flink对于经过转换操作之后的DataStream，提供了一些列的底层操作结构，能够帮我们实现数据流的手动重分区。为了与keyby相区别，我们把这些操作成为物理分区。物理分区与keyby另一大区别在于，keyby操作之后得到的是一个KeyedStream，而物理分区之后结果仍是DataStream，且流中的元素数据类型保持不变。这一点也可以看出，物理分区算子并不对数据进行转换，只是定义了数据的传输方式。
>
> 常见的物理分区策略有自定义分区（Custom Partitioning）、随机分区（Random Partitioning）、重缩放（Rescaling）、广播（Broadcasting）。

### Random Partitioning

**DataStream → DataStream **

随机分区是最简单的分区策略，将数据随机分配到下游算子的并行子任务中。分配过程按照均匀分布的策略，均匀的传递到下游。因为是完全随机的，所以对于同样的输入数据，每次执行得到的结果也不会相同，而且也做不到绝对的均匀，是整体来说会相对均匀。

```java
dataStream.shuffle();
```

### Round-Robin（Rebalancing）

**DataStream → DataStream **

轮询（Round-Robin）是Flink默认分区策略。，简单来说就是类似“发牌”的过程，按照先后顺序依次分发，可以将输入流数据平均的分配到下游算子的并行实例中。

```java
dataStream.rebalance();
```

### Rescaling

**DataStream → DataStream**

重缩放（Rescaling）与轮询分区非常相似，当进行重缩放分区时，其底层也是使用Round-Robin算法进行轮训，只不过参与轮训的下游子任务并不是所有的并发实例，而是其中的一部分。也就是说，“发牌人”如果有多个，那么轮训的方式是每个发牌人都面向所有人发牌；而重缩放的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。

具体如何分区要取决于上游和下游算子的并行度。

例如，如果上游算子的并行度为 2，下游算子的并行度为 6，则一个上游算子并发实例会将元素分配给下游算子其中的三个并发实例，而另一个上游算子并发实例会将元素分配给其他三个下游算子的并发实例。

另一方面，如果下游算子具有并行度 2，而上游算子具有并行度 6，则三个上游算子并发实例将分配给一个下游算子并发实例，而其他三个上游算子并发实例将分配给另一个下游算子并发实例。

很明显，当上下游算子并行度是整数倍时，重缩放的效率会更高。而在上下游算子的并行度不是整数倍的情况下，一个或多个下游算子并发实例将具有来自上游并发实例的不同数量的输入。

由于轮询时所有分区数据的重新平衡，当TaskManager数量较多时，这种跨节点的网络传输必然影响效率。而如果我们配置合适数量的slot数量，并配合用重缩放的方式进行局部分区，就可以让数据只在当前TaskManager的多个slot之间重新分配，从而避免网络传输。

![image-20240120234746057](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/image-20240120234746057.png)

```java
dataStream.rescale();
```

### Broadcasting

**DataStream → DataStream**

将数据流中的元素复制，并广播到下游的每个分区中。所以，当下游算子并行度大于1时，必然会数据重复，重复次数等于下游算子的并行度。适合于大数据集和小数据集做JOIN的场景。

```java
dataStream.broadcast();
```

### Global

**DataStream → DataStream**

全局分区是一种极端的分区方式，会将所有的输入流数据都发送到下游算子的第一个并发实例中去。这就相当于强行让下游算子并行度变为1。这个操作需要非常谨慎，可能对程序以及资源造成很大的压力。

```java
dataStream.global();
```

### Custom Partitioning

**DataStream → DataStream**

通过用户自定义的分区器（Partitioner），为流中的每一个元素选择下游的并发实例（执行线程）。

在调用`partitionCustom`方法时需要传入两个参数，第一个是自定义分区器对象，第二个是应用分区器的字段，它的指定方式与keyby指定key基本一样，可以通过字段名称指定，也可以通过字段位置索引来指定，还可以实现一个KeySelector。

```java
dataStream.partitionCustom(partitioner, "someKey");
dataStream.partitionCustom(partitioner, 0);
```

## Task Chaining and Resource Groups

算子链（Task Chaining），就是将两个连续的转换（算子）链接起来，一起放在同一个线程内执行，以获得更好的性能。Flink默认情况下是会尽可能链接各个算子的（比如链接两个连续的map算子）。如果需要，API也提供了对算子链的细粒度控制。

需要注意的是，算子链的相关方法必须在某个算子之后使用，因为他们的作用域为算子，而不是数据流。比如，你可以这样用：`someStream.map(...).startNewChain()`，但是不可以这样用：`someStream.startNewChain()`。

### Disable Chaining  In Whole Job

在整个作业中禁用算子链。

```java
StreamExecutionEnvironment.disableOperatorChaining()
```

### Disable Chaining For Specific Operator

比如禁用map算子的算子链。

```java
someStream.map(...).disableChaining();
```

### Start New Chain

创建新的算子链，链接该算子本身的前后两个算子。比如下面的示例，`startNewChain()`算子对其前后的两个`map()`算子进行链接，而该链接不包括最前面的`fileter()`算子。

```java
someStream.filter(...).map(...).startNewChain().map(...);
```

### Set Slot Sharing Group

设置slot共享组。Flink会将具有相同slot共享组的算子放入同一个slot中，同时将不具有相同slot共享组的算子放入其他slot中。这可用于slot之间的隔离。如果所有输入算子在同一个slot共享组中，则slot共享组从输入算子继承。默认slot共享组的名字是“default”，可以通过`slotSharingGroup()`方法，显式的将某个算子放入该组中。

```java
someStream.filter(...).slotSharingGroup("name");
```

### Name And Description

Flink中的算子以及job vertices有一个名字和描述。该名字和描述可以介绍算子和job vertices的具体实现功能，但他们的使用方式不同。

算子和job vertices的名字将用在web ui、线程名称、日志、指标等中使用。job vertices的名字是根据其中的算子名字构成的。名称要尽可能简洁，以避免对外部系统造成高压。

描述会用在执行计划中，并在web ui中显示为job vertices的详细信息。job vertices的描述是根据其中的算子的描述构成的。描述可以包含有关算子的详细信息，以便于运行时调试。

```java
someStream.filter(...).setName("filter").setDescription("x in (1, 2, 3, 4) and y > 1");
```

job vertex的描述默认格式为树形字符串。如果用户想将描述设置为以前版本的级联格式（cascading format），可以设置`pipeline.vertex-description-mode`为`CASCADING`。

默认情况下，Flink SQL生成的算子会有一个名字，该名字由算子的类型、id、以及详细的描述组成。如果用户想跟以前的版本一样，将名字设置为详细的描述，可以设置`table.optimizer.simplify-operator-name-enable`为`false`。

当pipeline的拓扑比较复杂时，用户可以设置`pipeline.vertex-name-include-index-prefix`为`true`，在vertex的名字中添加拓扑索引，这样就可以根据日志或指标标签轻松找到图中的vertex。

> 在 Apache Flink 中，`JobVertex` 是 `JobGraph` 中的一个基本概念，代表了数据流图中的一个操作或算子。每个 `JobVertex` 都对应于 Flink 作业中的一个操作，例如数据源、映射、筛选或聚合操作。`JobVertex` 的主要属性包括并行度和要执行的代码。在作业执行过程中，`JobManager` 会根据 `JobGraph` 来跟踪分布式任务、决定何时调度下一个任务（或一组任务），并对完成的任务或执行失败做出反应。
>
> `JobGraph` 被转换为 `ExecutionGraph`，后者是 `JobGraph` 的并行版本。每个 `JobVertex` 在 `ExecutionGraph` 中对应一个或多个 `ExecutionVertex`，每个 `ExecutionVertex` 跟踪特定子任务的执行状态。所有来自同一 `JobVertex` 的 `ExecutionVertices` 被保存在一个 `ExecutionJobVertex` 中，该对象跟踪整个操作的状态。
>
> 算子与JobVertex是同一个概念？
