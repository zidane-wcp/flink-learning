Flink程序通过各种source读取输入的数据，你可以通过`StreamExecutionEnvironment.addSource(sourceFunction)`将数据源附加到Flink程序里。

Flink提供了一些内置的`source function`，当然，你也可以通过实现`SourceFunction`接口实现非并行的source，或者通过实现`ParallelSourceFunction`、`RichParallelSourceFunction`接口实现可并行的source。

## Flink Predefined Sources

在`StreamExecutionEnvironment`类中，Flink提供了一些预先定义好的source，包括基于文件的、基于socket的、基于集合的以及Flink自定义的。以下只列出四种了，另外还包括`generateSequence` `fromSequence`等等，后续补充。

### File-based

* `readTextFile(path)` - 逐行读取文本文件，即符合`TextInputFormat`规范的文件，并将这些行作为字符串返回。
* `readFile(fileInputFormat, path)` - 按照指定的输入格式，一次性读取文件内容。
* `readFile(fileInputFormat, path, watchType, interval, typeInfo)` - 该方法是前两个方法内部调用的方法，前两个方法都是对该方法的封装。它根据指定的输入格式，读取指定路径的文件内容。watchType是一个`FileProcessingMode`枚举对象，表示是只读取文件当前内容(`FileProcessingMode.PROCESS_ONCE`)，还是会周期性的扫描文件的新数据(`FileProcessingMode.PROCESS_CONTINUOUSLY`)， interval就是扫描的间隔。

其中，`readFile(fileInputFormat, path, watchType, interval, typeInfo)`方法是Flink DataStream API中最底层的读取文件的API，其他文件读取API都是对该方法的封装。在该方法中，调用了作用域为`private`的`createFileInput()`方法，`createFileInput()`中使用`env.addSource()`将source添加到执行环境中，并`return new DataStreamSource<>(source);`，也就是说文件source最终都是通过`env.addSource()`添加source的。

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
* `fromElements(OUT...)` - 将OUT转换为集合，并从可变长参数的第一个对象中抽取TypeInformation，再调用`fromCollection(Collection, TypeInformation)`。可变长参数中提供的数据必须具有相同的类型，即OUT类型。
* `fromElements(Class, OUT...)` - 将OUT转换为集合，并从Class中抽取TypeInformation，再调用`fromCollection(Collection, TypeInformation)`。可变长参数中提供的数据必须具有相同的类型，即OUT类型。
* `fromParallelCollection(SplittableIterator, Class)` - 从迭代器创建并行的数据流，参数class用来抽取TypeInformation，返回TypeInformation类型的数据流
* `fromSequence(from, to)` - 在指定的范围内，生成并行的数字序列。
* `generateSequence(from, to)` - 在指定的范围内，生成并行的数字序列。该方法已经`@Deprecated`，用`fromSequence(from, to)`代替。

### Custom

* `addSource` - 这是`StreamExecutionEnvironment`类中的一个方法，用于将自定义的SourceFunction附加到执行环境中，并创建数据流。比如，如果要读取kafka中的数据，可以通过`addSource(new FlinkKafkaConsumer<>(...))`。