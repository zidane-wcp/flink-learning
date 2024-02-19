

数据源（Data Sources）是Flink程序用来读取数据的组件，可以将外部数据读入Flink程序中并应用各种Transformation进行处理。在Flink当前版本中，支持两套数据源API，一套是基于`SourceFunction`接口，通过`env.addSource(SourceFunction)`方法引入；另一套是基于`Source`接口，通过`env.fromSource(Source)`引入，先大概介绍下这两套API的不同。

**`SourceFunction`**：
`SourceFunction`是Flink早期提供的一套API，实现该接口可以实现自定义的非并行的source，也可以通过实现`ParallelSourceFunction`接口实现并行的source，另外这两个接口都有其对应的*RichFunction*，分别是`RichSourceFunction`和`RichParallelSourceFunction`。在Flink1.18版本中这套API被标记为`@deprecated`，应尽量避免使用这套API。

**`Source`**：
基于`Source`接口的这套API在Flink1.11版本中作为全新的Source-API被引入，与`SourceFunction`相比，这套API允许在`fromSource()`调用时，指定时间戳提取以及watermark生成策略。Flink CDC采用的就是这套API。而这套API将数据源API从Function体系中剥离出来，归入Connector体系，详情请参考[FLINK-10740](https://issues.apache.org/jira/browse/FLINK-10740)，[GitHub Pull Request #10486a](https://github.com/apache/flink/pull/10486)、[FLINK-15131](https://issues.apache.org/jira/browse/FLINK-15131)，关于`Source`的设计及原理请参考[FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)。

接下来，首先介绍Flink内置的数据源api，然后再分别介绍下`SourceFunction`和`Source`接口的基本原理，以及如何借助这两个接口实现自定义source。

## Flink Predefined Source API

在`StreamExecutionEnvironment`类中，Flink提供了一些预先定义好的source，包括基于文件的、基于socket的、基于集合的。这些内置的source中，只有`fromSequence(from, to)`是基于新API`Source`实现的，其他都是基于`SourceFunction`实现。

### File-based

* `readTextFile(path)` - 逐行读取文本文件，即符合`TextInputFormat`规范的文件，并将这些行作为字符串返回。
* `readFile(fileInputFormat, path)` - 按照指定的输入格式，一次性读取文件内容。
* `readFile(fileInputFormat, path, watchType, interval, typeInfo)` - 该方法是前两个方法内部调用的方法，前两个方法都是对该方法的封装。它根据指定的输入格式，读取指定路径的文件内容。watchType是一个`FileProcessingMode`枚举对象，表示是只读取文件当前内容(`FileProcessingMode.PROCESS_ONCE`)，还是会周期性的扫描文件的新数据(`FileProcessingMode.PROCESS_CONTINUOUSLY`)， interval就是扫描的间隔。

其中，`readFile(fileInputFormat, path, watchType, interval, typeInfo)`方法是Flink DataStream API中最底层的读取文件的API，其他文件读取API都是对该方法的封装。在该方法中，调用了作用域为`private`的`createFileInput()`方法，`createFileInput()`中使用`env.addSource()`将source添加到执行环境中，并`return new DataStreamSource<>(source);`，也就是说文件source都是基于`SourceFunction`接口的。

在底层，也就是在`createFileInput()`方法中，Flink 将文件读取过程拆分为两个子任务，即*directory monitoring*和*data reading*，这两个子任务独立进行。*directory monitoring*是由单个**非并行**（并行度 = 1）任务实现的，而*data reading*是由并行的多个任务执行的。*data reading*的并行度等于作业并行度。*directory monitoring*子任务的作用是扫描目录（周期性或仅一次，具体取决于`watchType`），找到要处理的文件，将它们划分为split，并将这些 split 分配给下游*data reading*，*data reading*来读取数据。每个split只能由一个*data reading*读取，而一个*data reading*可以逐一阅读多个split。

*directory monitoring*在Flink源码中就是`ContinuousFileMonitoringFunction<OUT>`类，该类继承了`RichSourceFunction`抽象类，并实现了`CheckpointedFunction`接口。

> 重要：
>
> 1. 如果`watchType`设置为`FileProcessingMode.PROCESS_CONTINUOUSLY`，当文本文件被修改时，其内容将被全部重新处理。这将破坏“exactly-once”语义，因为在文件末尾附加数据将导致其所有内容被重新处理。
> 2. 如果`watchType`设置为`FileProcessingMode.PROCESS_ONCE`，*directory monitoring*扫描一次路径后就推出，不会等待读取器读取文件内容。当然，读取器会继续读取文件内容，直到全部内容都读取完毕。*directory monitoring*退出后就不会再有checkpoint了，此时若某个节点挂掉了，作业将会从更早的checkpoint恢复，导致增加恢复时间。

### Socket-based

* `socketTextStream(String hostname, int port)` - 从端口中读取数据。可以用分隔符来分割元素。该方法有多个变种，除了可以指定hostname和port外，还可以指定delimiter和maxRetry，具体请参考Flink源码 [StreamExecutionEnvironment.java](https://github.com/apache/flink/blob/release-1.15.4/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java)。

作业启动前，需要先从终端使用 netcat 启动输入流，否则作业启动报错：
```bash
nc -lk 9999
```

### Collection-based

* `fromCollection(Collection)` - 从Java.util.Collection集合中创建数据流，集合中的所有元素必须具有相同的数据类型。该方法会从集合的第一个元素中提取出TypeInformation，并返回TypeInformation类型的数据流。
* `fromCollection(Collection, TypeInformation)` - 从Java.util.Collection集合中创建数据流，并返回指定类型TypeInformation的数据流。
* `fromCollection(Iterator, Class)` - 从迭代器创建数据流，从Class中抽取出TypeInformation，并返回TypeInformation类型的数据流。
* `fromCollection(Iterator, TypeInformation)` - 从迭代器创建数据流，并返回TypeInformation类型的数据流。
* `fromElements(OUT...)` - 将OUT转换为集合，并从可变长参数的第一个对象中抽取TypeInformation，再调用`fromCollection(Collection, TypeInformation)`。可变长参数中提供的数据必须具有相同的类型，即OUT类型。`fromElements()`是对`fromCollection`的封装。
* `fromElements(Class, OUT...)` - 将OUT转换为集合，并从Class中抽取TypeInformation，再调用`fromCollection(Collection, TypeInformation)`。可变长参数中提供的数据必须具有相同的类型，即OUT类型。`fromElements()`是对`fromCollection`的封装。
* `fromParallelCollection(SplittableIterator, Class)` - 从迭代器创建并行的数据流，参数class用来抽取TypeInformation，返回TypeInformation类型的数据流。
* `fromSequence(from, to)` - 在指定的范围内，生成并行的数字序列，该方法基于数据源新API`Source`接口。
* `generateSequence(from, to)` - 在指定的范围内，生成并行的数字序列。该方法是基于`SourceFunction`的，已经`@Deprecated`，用`fromSequence(from, to)`代替。

## The SourceFunction API && The addSource Operator

这套API所涉及到的Function的源码见 [org.apache.flink.streaming.api.functions.source](https://github.com/apache/flink/tree/release-1.18.0/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source)。

另外，基于`SourceFunction`、`RichSourceFunction`、`ParallelSourceFunction`、`RichParallelSourceFunction`，分别实现了自定义source的示例代码，代码见code目录。

## The Data Source API && The fromSource Operator

这套API所涉及到的connector的源码见 [org.apache.flink.api.connector.source](https://github.com/apache/flink/tree/release-1.18.0/flink-core/src/main/java/org/apache/flink/api/connector/source)。

本节介绍Flink新的Data Source API，包括其基本概念、原理、架构。还将介绍如何基于Data Source API实现自定义数据源。

基于Data Source API，Flink实现了一些内置的数据源，新版本也叫Connector，详见 [Connector Docs](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/overview/)。

### Data Source Concepts

 **Core Components**

Data Source有三个核心组件，分别是：*Splits*、*SplitEnumerator*、 *SourceReader*。

* **Split** - Data Source会对读取的数据进行分片，一个Split就是一个数据的分片，是数据的一部分，例如一个文件或日志的分区。Split是Data Source进行任务分配和数据并行读取的基本单位。
* **SourceReader** - 源读取器，用于请求分片，读取并处理分片。SourceReader在Task Managers的`SourceOperators`算子中并行执行，并产生并行的事件流/记录流。
* **SplitEnumerator** - 分片枚举器，用于生成Splits，并将它们分发给SourceReaders。它在Job Manager上以单并行度执行，负责对未分发的Splits进行维护，并将它们均衡的分发给各个SourceReader。

`Source`接口作为API入口，将以上三个组件组合在一起。

![Illustration of SplitEnumerator and SourceReader interacting](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/source_components.svg)



**Unified Across Streaming and Batch**

Data Source API以统一的方式，支持无界的流数据源和有界的批数据源。

事实上，这两种数据源的区别不大：对于有界批数据源，SplitEnumerator会生成一组固定的Splits，并且每个Split必须是有限的；而对于无界流数据源，Split是无限的，并且SplitEnumerator会源源不断的产生新的Splits。

以下是对File Source和Kafka Source实现的简单介绍，以说明数据源组件在流和批处理情况下是如何交互的。请注意，为了便于说明，以下概念均进行了简化，真实实现要更复杂一些。

**Bounded File Source**

Data Source要提供读取的文件的URL或目录，以及*Format*格式信息，以确定如何解析文件。

* 一个Split就是一个文件，或是文件的某个区域（如果Format支持文件分片）。
* SplitEnumerator列出给定目录路径下的所有文件。它会将Splits分配给下一个请求Split的SourceReader。一旦分配了所有Splits，它就会使用*NoMoreSplits*来响应请求。
* SourceReader请求一个Split并读取分配的Split（文件或文件区域），并使用给定的Format对其进行解析。如果它的请求得到的是一个NoMoreSplits消息，那么它就执行完成了。

**Unbounded Streaming File Source**

工作方式与Bounded File Source相同，只是SplitNumerator从不使用NoMoreSplits进行响应，而是定期列出给定URI/Path下的内容以检查新文件。一旦找到新文件，它就会为它们生成新的Split，并可以将它们分配给可用的SourceReaders。

**Unbounded Streaming Kafka Source**

Data Source要提供一个Kafka Topic，或是Topic的列表或正则表达式，以及用于解析Kafka记录的反序列化器。

* 一个Split就是一个Kafka Topic分区。
* SplitEnumerator会连接到Kafka Broker，并列出订阅的Topic中所有的分区。SplitEnumerator可以选择性的重复此操作，以发现新添加的Topic或Partition。
* SourceReader使用KafkaConsumer读取分配的Splits（Topic Partitions），并使用提供的反序列化器反序列化记录。Splits（Topic Partitions）没有终点，因此SourceReader永远不会到达数据的结束。

**Bounded Kafka Source**

工作方式与Unbounded Streaming Kafka Source相同，只是每个Splits（Topic Partitions）都有一个定义好的end offset。一旦SourceReader达到Split的这个end offset，它就会完成该Split。完成所有分配的Split后，SourceReader就执行完成了。

### The Data Source API

本节介绍FLIP-27中引入的新Source API的主要接口，并为开发人员提供有关Source开发的提示。

#### The Source Interface

`Source`接口是The Data Source API的入口，是一个工厂设计模式风格的接口，用于创建以下组件：

- *Split Enumerator*
- *Source Reader*
- *Split Serializer*
- *Enumerator Checkpoint Serializer*

此外，为实现流批一体，Flink将批当成一种特殊的流来处理，`Source`接口提供了数据源的*Boundedness*属性，批既是*BOUNDED*有界的，流既是*CONTINUOUS_UNBOUNDED*无界的，以便Flink可以选择适当的模式来运行Flink作业。

`Source`的实现应该是可序列化的，因为`Source`实例在运行时被序列化并上传到Flink集群。

源码见 https://github.com/apache/flink/blob/release-1.15.4/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java 。

#### The SplitEnumerator Interface

`SplitNumerator`接口被认为是Source的“大脑”，用于splits的发现与分配。SplitEnumerator的典型实现方式如下：

* `SourceReader`注册处理

* `SourceReader`失败处理

    当`SourceReader`失败时，将调用`addSplitsBack()`方法。`SplitEnumerator`应该回收那些`SourceReader`失败的Splits。

* `SourceEvent`处理

    `SourceEvent`是一个接口，其实例是在`SplitEnumerator`和`SourceReader`之间发送的自定义事件。其实现可以利用这一机制进行复杂的协调。

* Split discovery and assignment

    `SplitEnumerator`可以将Splits分配给`SourceReader`，并响应各种事件，包括发现新Splits、新`SourceReader`注册、`SourceReader`失败等。

`SplitEnumeratorContext`接口在`SplitEnumerator`的创建或恢复时提供给`Source`，`SplitEnumeratorContext`可以帮助`SplitEnumerator`完成上述的所有工作。SplitEnumeratorContext允许SplitEnumerator检索SourceReader的必要信息并执行协调操作。Source实现应将SplitEnumeratorContext传递给SplitEnumerator实例。

虽然`SplitEnumerator`实现可以通过在调用其方法时仅采取协调操作的被动方式很好地工作，但一些`SplitEnumerator`实现可能希望主动采取操作。例如，`SplitEnumerator`可能希望定期执行Splits发现，并将新的Splits分配给`SourceReader`。可以通过`SplitEnumeratorContext`接口中的`callAsync()`方法很方便的实现，如下的代码片段展示了如何在不维护自己的线程的情况下实现这一点。

```java
class MySplitEnumerator implements SplitEnumerator<MySplit> {
    private final long DISCOVER_INTERVAL = 60_000L;

    /**
     * A method to discover the splits.
     */
    private List<MySplit> discoverSplits() {...}
    
    @Override
    public void start() {
        ...
        enumContext.callAsync(this::discoverSplits, splits -> {
            Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
            int parallelism = enumContext.currentParallelism();
            for (MockSourceSplit split : splits) {
                int owner = split.splitId().hashCode() % parallelism;
                assignments.computeIfAbsent(owner, new ArrayList<>()).add(split);
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignments));
        }, 0L, DISCOVER_INTERVAL);
        ...
    }
    ...
}
```

#### SourceReader

`SourceReader`组件运行在Task Manager之上，用于消费Splits中的记录。

`SourceReader`暴露了一个基于拉取（pull-based）的消费接口。Flink Task在循环中不断调用`InputStatus pollNext(ReaderOutput<T> var1)`方法来轮询`SourceReader`中的记录，该方法的返回值`InputStatus`是一个枚举对象，表示`SourceReader`的当前状态，有以下取值：

* `MORE_AVAILABLE` - `SourceReader`有更多的可用记录。
* `NOTHING_AVAILABLE` - `SourceReader`当前没有可用记录，但将来可能有更多记录。
* `END_OF_INPUT` - `SourceReader`中的所有记录都被消费完，达到了数据的末尾，意味着`SourceReader`实例可以被关闭了。

为了提高性能，为`pollNext(ReaderOutput)`提供了一个`ReaderOutput`参数，因此如果必须的话，对于每次调用`pollNext()`方法时`SourceReader`可以发出多条记录。例如，有些外部系统的最小工作粒度是block，每个block可能包含多条记录，所以source只能在每个block的边界处进行checkpoint。这种情况下，`SourceReader`可以一次将一个block中的所有记录发送给`ReaderOutput`（由`ReaderOutput.collect()`进行收集）。

然而，非必要情况下，在实现`SourceReader`时，在单次`pollNext(ReaderOutput)`的调用中不要发送多条记录，因为这时task线程会在一个循环中从`SourceReader`中拉取记录，该循环无法被组织。

`SourceReader`的所有state都应维护在`SourceSplit`中，以便在`snapshotState()`方法被调用时返回，还可以在需要时将`SourceSplit`重新分配给其他`SourceReader`。

在创建`SourceReader`时，可以将`SourceReaderContext`对象提供给`Source`，这样，`Source`就可以将context上下文传递给`SourceReader`实例，见源码`SourceReader<T, SplitT> createReader(SourceReaderContext var1)`。`SourceReader`可以通过`SourceReaderContext`将`SourceEvent`发送给`SplitEnumerator`。`Source`的一个典型设计模式是让`SourceReader`的的本地信息传递给`SplitEnumerator`，这样`SplitEnumerator`就可以利用这个全局视图来做决定。

`SourceReader` API是一个底层的API，它允许用户手动处理Splits，并拥有自己的线程模型来获取和移交记录。为了方便`SourceReader`的实现，Flink提供了`SourceReaderBase`类，它大大减少了实现`SourceReader`接口所需的工作量。强烈建议开发人员基于`SourceReaderBase`类来实现`SourceReader`，而不是从头编写。更多细节请参考下面的小节《The Split Reader API》。

#### Use the Source

为从`Source`创建数据流，需要将`Source`对象传递给`StreamExecutionEnvironment`，例如：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Source mySource = new MySource(...);

DataStream<Integer> stream = env.fromSource(
        mySource,
        WatermarkStrategy.noWatermarks(),
        "MySourceName");
...
```

### The Split Reader API

在Data Source API中，核心的SourceReader API是完全异步的，需要用户自己实现异步分片读取。然而，实际情况是，大多数数据源的操作都是同步的，例如，客户端执行`poll()`调用是同步的（KafkaConsumer），分布式文件系统上的IO操作也是同步的（如HDFS、S3）。为了使其与异步的Source API兼容，这些同步操作需要在单独的线程中执行，这些线程将数据移交给Source Reader的异步部分。

与`Source` API相比，[SplitReader](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/splitreader/SplitReader.java) 是更高层次的API，可以简单的实现对数据源的同步的读取或轮询，比如读取Kafka。

这套API的核心是`SourceReaderBase`抽象类，它采用SplitReader并创建运行SplitReader的fetcher线程，支持不同的消耗线程模型。

#### SplitReader

`SplitReader`接口只有三个方法：

* `fetch()` - 一个同步方法，用于返回`RecordsWithSplitIds`对象。
* `handleSplitsChanges()` - 一个异步方法，用于处理Splits更改。
* `wakeUp()` - 一个异步方法，用于唤醒阻塞的fetch操作。

`SplitReader`只聚焦于从外部系统读取记录，比`SourceReader`简单很多。

#### SourceReaderBase

对于底层的`Source`接口API，其`SourceReader`接口的实现通常需要完成以下操作：

* 用一个线程池以同步的方式从外部系统的Splits中获取记录。
* 处理内部fetch线程和其他方法调用（如`pollNext(ReaderOutput)`）之间的同步。
* 维护每个Split的水印以进行水印对齐。
* 维护每个Split的状态以进行checkpoint。

为了减少编写新`SourceReader`带来的工作量，Flink提供了一个`SourceReaderBase`抽象类作为`SourceReader`接口的基本实现。SourceReaderBase已经开箱即用地完成了上述所有工作。要编写一个新的`SourceReader`，只需要继承`SourceReaderBase`抽象类，然后填充很少的几个方法，并实现高层次的`SplitReader`接口。

#### SplitFetcherManager

`SourceReaderBase`支持一些开箱即用的线程模型，这取决于它所使用的`SplitFetcherManager`的行为。The `SplitFetcherManager` helps create and maintain a pool of `SplitFetcher`s each fetching with a `SplitReader`。它还决定如何向每个split fetcher分配splits。

例如，如下图所示，每个`SplitFetcherManager`可能拥有固定数量的线程，每个线程从splits中获取并分配给`SourceReader`。

![One fetcher per split threading model.](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/source_reader.svg)

下面的代码片段实现了该线程模型。

```java
/**
 * A SplitFetcherManager that has a fixed size of split fetchers and assign splits 
 * to the split fetchers based on the hash code of split IDs.
 */
public class FixedSizeSplitFetcherManager<E, SplitT extends SourceSplit> 
        extends SplitFetcherManager<E, SplitT> {
    private final int numFetchers;

    public FixedSizeSplitFetcherManager(
            int numFetchers,
            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(futureNotifier, elementsQueue, splitReaderSupplier);
        this.numFetchers = numFetchers;
        // Create numFetchers split fetchers.
        for (int i = 0; i < numFetchers; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // Group splits by their owner fetchers.
        Map<Integer, List<SplitT>> splitsByFetcherIndex = new HashMap<>();
        splitsToAdd.forEach(split -> {
            int ownerFetcherIndex = split.hashCode() % numFetchers;
            splitsByFetcherIndex
                    .computeIfAbsent(ownerFetcherIndex, s -> new ArrayList<>())
                    .add(split);
        });
        // Assign the splits to their owner fetcher.
        splitsByFetcherIndex.forEach((fetcherIndex, splitsForFetcher) -> {
            fetchers.get(fetcherIndex).addSplits(splitsForFetcher);
        });
    }
}
```

利用该线程模型，`SourceReader`可实现如下：

```java
public class FixedFetcherSizeSourceReader<E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public FixedFetcherSizeSourceReader(
            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                futureNotifier,
                elementsQueue,
                new FixedSizeSplitFetcherManager<>(
                        config.getInteger(SourceConfig.NUM_FETCHERS),
                        futureNotifier,
                        elementsQueue,
                        splitFetcherSupplier),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Collection<String> finishedSplitIds) {
        // Do something in the callback for the finished splits.
    }

    @Override
    protected SplitStateT initializedState(SplitT split) {
        ...
    }

    @Override
    protected SplitT toSplitType(String splitId, SplitStateT splitState) {
        ...
    }
}
```

在`SplitFetcherManager`和`SourceReaderBase`之上，可以轻松实现具有其自己的线程模型的`SourceReader`实现类。

