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

Flink 还通过以下function对转换后的精确流分区提供更底层的控制（如果需要）。

### Custom Partitioning



