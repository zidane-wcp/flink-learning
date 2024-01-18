Flink 中的 DataStream 程序，可以实现数据流转换（例如，过滤、更新状态、定义窗口、聚合）。

数据流最初是从各种source（例如消息队列、套接字流、文件）创建的。数据流在创建后，可以经过各种 transformation，将结果发送给 sink，sink 可以将其写入各种外部系统，例如可以将数据写入文件或标准输出（例如命令行终端）。简单说，就是数据流会经过source、transformation、sink三个过程。后面的章节将通过具体的代码展示这个过程。

Flink 程序可以在各种环境中运行，可以独立运行，也可以嵌入其他程序中。执行可以发生在本地 JVM 中，也可以发生在许多机器的集群上。

## 什么是DataStream？

DataStream API 的名称来自于一个特殊的 DataStream 类，该类用于表示 Flink 程序中的数据集合。可以将它们视为可以包含重复项的不可变数据集合。这些数据可以是有限的，也可以是无界的，但是可以用同一套API来处理他们，这就是流批一体。

DataStream在某些方面的用法与Java中的集合很相似，但在某些关键方面有很大不同。DataStream是不可变的，这意味着一旦创建它们，就无法添加或删除元素。也不能简单地操作内部元素，而只能使用DataStream API 操作（也称为转换）对其进行处理。

可以通过在 Flink 程序中添加source来创建 DataStream。然后可以从中派生新的流，并通过类似于map、filter的方法去组合并处理这些流。

## Flink程序剖析

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

数据源就是将文件数据或其他外部数据读入Flink程序中，并添加到执行环境中，便可以用各种API进行转换。

比如，想要获取文件系统中的文件数据，Flink提供了不同的方法来进行：可以按行读取、读取CSV文件或其他source。下面的实例展示了按行读取的用法：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = env.readTextFile("file:///path/to/file");
```

### Transformation

读取到数据源后，你将得到一个`DataStream`，然后就可以在这个`DataStream`进行转换操作。比如，map操作：

```java
DataStream<String> input = ...;

DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
```

这个map操作将数据流中的字符串转换为整型。

### Sink

当完成了最终的转换之后，可以将这个数据流写入其他外部系统，或者打印到控制台，这就是sink。比如写入文件、打印到控制台：

```java
writeAsText(String path);

print();
```

###  Program execution

如果你的程序中数据流的pipeline已经定义完毕，你需要调用`StreamExecutionEnvironment`类中的`execute()`方法才会触发程序的执行。根据执行环境的不同，程序可能在本地机器上执行，也可能在某个Flink集群上执行。

`execute()`方法将等待Flink作业（job）执行完，并返回一个`JobExecutionResult`对象，该对象包含执行时间和累加器结果。

如果你不想等待作业的执行，你可以调用`StreamExecutionEnvironment`类的`executeAsync()`方法异步触发程序的执行。该方法将立刻返回一个`JobClient`对象，该对象可以用来获取`JobExecutionResult`，与`execute()`方法返回的`JobExecutionResult`一样。

```java
final JobExecutionResult jobExecutionResult = env.execute();
//or
final JobClient jobClient = env.executeAsync();

final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
```

关于程序执行的最后一部分对于理解 Flink 的转换操作何时以及如何执行至关重要。所有Flink程序都是延迟执行的：当程序的main方法执行时，数据加载和转换不会直接发生。相反，每个操作都会被创建并添加到数据流图中。当执行由`execute()`执行环境上的调用显式触发时，操作才会实际执行。程序是在本地执行还是在集群上执行取决于执行环境的类型。

这种延迟执行让你可以构建复杂的程序，Flink 将其作为一个整体规划的单元来执行。



















