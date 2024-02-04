Flink 中的 DataStream 程序，可以实现数据流转换（例如，过滤、更新状态、定义窗口、聚合）。


数据流最初是从各种source（例如消息队列、套接字流、文件）创建的。数据流在创建后，可以经过各种 transformation，将结果发送给 sink，sink 可以将其写入各种外部系统，例如可以将数据写入文件或标准输出（例如命令行终端）。简单说，就是数据流会经过source、transformation、sink三个过程，后面的章节将进行详细介绍。


Flink 程序可以在各种环境中运行，可以独立运行，也可以嵌入其他程序中。执行可以发生在本地 JVM 中，也可以运行在多个机器的集群上。

## What is a DataStream?


DataStream API 的名称来自于一个特殊的 `DataStream` 类，该类用于表示 Flink 程序中的数据集合。可以将它们视为可以包含重复项的不可变数据集合。这些数据可以是有限的，也可以是无界的，但是可以用同一套API来处理他们，这就是流批一体。


DataStream在某些方面的用法与Java中的集合很相似，但在某些关键方面有很大不同。DataStream是不可变的，这意味着一旦创建它们，就无法添加或删除元素。也不能简单地操作内部元素，而只能使用DataStream API 操作（也称为转换）对其进行处理。


可以通过在 Flink 程序中添加source来创建 DataStream，并通过类似于map、filter的方法去组合并处理这些DataStream，然后可以派生出新的流。

## Anatomy of a Flink Program


Flink程序是用来操作DataStream的，包括创建、转换、输出。每个Flink程序都包含以下几个基本的部分：


1. 获取执行环境（execution environment）。

2. 创建数据源（source）。

3. 对数据源进行转换操作（transformation）。

4. 指定转换结果的输出位置（sink）。

5. 触发程序的执行（ program execution）。


接下来对每个部分进行介绍。这些基本部分中所涉及到的大部分Java DataStream API的核心类都位于[org.apache.flink.streaming.api ](https://github.com/apache/flink/blob/release-1.15//flink-streaming-java/src/main/java/org/apache/flink/streaming/api)。


### Execution environment


执行环境（Execution environment）是所有Flink程序的基础，可通过`StreamExecutionEnvironment`类中的以下三个静态方法来获取执行环境：


```java
getExecutionEnvironment();

createLocalEnvironment();

createRemoteEnvironment(String host, int port, String... jarFiles);
```


通常情况下，你用`getExecutionEnvironment()`方法就可以了，该方法将根据上下文，来决定如何获取Execution environment。比如，你的程序运行在IDE中，那么该方法将在你本地的机器上创建本地执行环境，你的程序将运行在该本地环境中。如果你将你的程序打包成Jar包，提交到Flink集群去运行，那么Flink集群管理器将执行Jar包中的`main()`方法，并通过`getExecutionEnvironment()`方法返回集群上的执行环境。


### Source


数据源source就是将文件数据或其他外部数据读入Flink程序中，并添加到执行环境中，便可以用各种API进行转换。


比如，想要获取文件系统中的文件数据，Flink提供了不同的方法来进行：可以按行读取、读取CSV文件或其他source。下面的示例代码展示了按行读取的用法：


```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = env.readTextFile("file:///path/to/file");
```


### Transformation


读取到数据源后，你将得到一个`DataStream`，然后就可以在这个`DataStream`进行转换操作。比如，如下的map操作，将数据流中的字符串转换为整型：


```java
DataStream<String> input = ...;

DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
```


### Sink


当完成了最终的转换之后，可以将这个数据流写入其他外部系统，或者打印到控制台，这就是sink。比如写入文件、打印到控制台：


```java
datastream.writeAsText(String path);

datastream.print();
```


###  Program execution


Flink程序中数据流的pipeline定义完毕后，你需要调用`StreamExecutionEnvironment`类中的`execute()`方法才会触发程序的执行。根据执行环境的不同，程序可能在本地机器上执行，也可能在某个Flink集群上执行。

`execute()`方法将等待Flink作业（job）执行完，并返回一个`JobExecutionResult`对象，该对象包含执行时间和累加器结果，如以下`JobExecutionResult`类的源码：

```java
@Public
public class JobExecutionResult extends JobSubmissionResult {
    private final long netRuntime;
    private final Map<String, OptionalFailure<Object>> accumulatorResults;
	...
}
```


如果你不想等待作业的执行，你可以调用`StreamExecutionEnvironment`类的`executeAsync()`方法异步触发程序的执行。该方法将立刻返回一个`JobClient`对象，该对象可以用来获取`JobExecutionResult`，与`execute()`方法返回的`JobExecutionResult`一样。


```java
final JobExecutionResult jobExecutionResult = env.execute();

//or
final JobClient jobClient = env.executeAsync();
final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
```

关于程序执行的最后一部分对于理解 Flink 的转换操作何时以及如何执行至关重要。所有Flink程序都是延迟执行的：当程序的main方法执行时，数据加载和转换不会直接发生，此时只是根据指定的算子创建transformation，并将其添加到数据流pipeline中。当在你指定的执行环境中调用`execute()`方法时，才会触发各个算子的真正执行。程序是在本地执行还是在集群上执行取决于执行环境的类型。


这种延迟执行让你可以构建复杂的程序，Flink 将其作为一个整体规划的单元来执行。

## Example Program

以下程序是一个完整的Flink应用，流式窗口统计word count，计算web socket中不同单词的数量。

```java
package org.wcp.flink.operators.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
```

运行该程序之前，请首先从终端使用 netcat 启动输入流：

```bash
nc -lk 9999
```

只需输入一些单词，然后按回车键即可将单词输入到该Flink程序中。

## Execution Parameters

通过`StreamExecutionEnvironment`类，我们可以设置作业的运行时配置。所有的配置项及其说明请参考Flink官方文档 [execution configuration](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/config/) 。

对于不同的配置项，可能配置的方式不太一样，大概有以下两种配置方式：

* 利用`StreamExecutionEnvironment`类中的`ExecutionConfig`对象，例如：`env.getConfig().setAutoWatermarkInterval(1000)`。
* 直接在`StreamExecutionEnvironment`对象中进行配置，例如：`env.setBufferTimeout(1000)`。对于某些配置项，这种配置方式是对`ExecutionConfig`类的封装。

### Fault Tolerance Configuration

Flink的容错是通过checkpoint机制保证的，关于checkpoint机制的相关配置请参考Flink官方文档：[State & Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/fault-tolerance/checkpointing/) 。

## Controlling Latency

默认情况下，元素不会在网络中一个一个的传输，这会造成不必要的网络拥挤，所以应该对元素进行缓冲，批量传输。缓冲区的大小可以在Flink的配置文件中进行配置。缓冲机制可以提高吞吐量，但也会带来延迟，因为数据流中的元素不是立刻就发送的。为了平衡吞吐量和延迟，你可以在执行环境中调用`env.setBufferTimeout(timeoutMillis)`设置缓冲区在填满之前的最长超时时间，在该超时时间到期后，即使缓冲区未填满也会发送数据，该值默认为100ms。

```java
env.generateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
```

为了最大化吞吐量，可以将缓冲区超时时间设置为-1，即`setBufferTimeout(-1)`，这将移除过期机制，而是缓冲区满了之后才会发送数据。为了最小化延迟，你可以将该超时时间设置为接近于0的某个值，例如5毫秒或10毫秒，但是最好不要设置为0，因为这可能导致严重的性能下降。

## Debugging

在分布式集群中运行流式程序之前，最好确保实现的算法按需工作。因此，实现数据分析程序通常是一个检查结果、调试和改进的渐进过程。

Flink通过支持IDE中的本地调试、测试数据的注入和结果数据的收集，提供了显著简化数据分析程序开发过程的功能。本节给出了一些如何简化Flink程序开发的提示。

### Local Execution Environment

`LocalStreamEnvironment`在创建它的同一JVM进程中启动Flink系统。如果你从IDE启动LocalEnvironment，你可以在代码中设置断点，并轻松调试程序：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

DataStream<String> lines = env.addSource(/* some source */);
// build your program

env.execute();
```

### Collection Data Sources

Flink提供了由Java集合支持的特殊数据源，以简化测试。一旦测试了程序，就可以很容易地用从外部系统读取/写入的source和sink进行替换：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

// Create a DataStream from a list of elements
DataStream<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

// Create a DataStream from any Java collection
List<Tuple2<String, Integer>> data = ...
DataStream<Tuple2<String, Integer>> myTuples = env.fromCollection(data);

// Create a DataStream from an Iterator
Iterator<Long> longIt = ...;
DataStream<Long> myLongs = env.fromCollection(longIt, Long.class);
```

注意：目前，集合数据源要求数据类型和迭代器实现`Serializable`接口。此外，集合数据源不能并行执行（并行度=1）。

### Iterator Data Sink

Flink还提供了一个iterator sink来收集DataStream的计算结果，用于测试和调试：

```java
import org.apache.flink.streaming.experimental.DataStreamUtils;

DataStream<Tuple2<String, Integer>> myResult = ...;
Iterator<Tuple2<String, Integer>> myOutput = DataStreamUtils.collect(myResult);
```



