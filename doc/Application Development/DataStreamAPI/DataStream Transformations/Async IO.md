## Asynchronous I/O for External Data Access

本节描述Flink API中与外部数据存储系统采用异步IO交互的相关概念。关于异步IO的设计和实现可以参考Flink的设计文档：[FLIP-12: Asynchronous I/O Design and Implementation](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65870673)。

## The need for Asynchronous I/O Operations

当与在Flink中与外部系统交互时，比如要使用外部数据库中存储的数据对数据流进行丰富，此时需要注意的是Flink应用的主要工作不是用来等待与外部系统的通讯延迟，不应在与外部系统的交互中占用太多时间。

例如在`MapFunction`中直接访问外部数据库，这就是同步交互模式：`MapFunction`将请求发送到外部数据库，并等待其返回。很多场景下，等待请求返回将会占用算子的大多数时间。

与外部数据库的异步交互，就是在单并行度的算子中，每次可以同时发出多个请求，并同时接收多个响应。这样，利用单个等待时间，可以处理多个请求和响应，在大多数场景下都可以大大提高吞吐量。

![img](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/async_io.svg)

注意，虽然可以将`MapFunction`并行度调大以提高吞吐量，但是大并行度意味着更高的资源消耗：更多的task、更多的县城、更多的Flink内部网络连接、更多的外部数据库连接、更多的缓冲池，以及内部元数据存储开销。

## Prerequisites

对数据库（或键/值存储）实现适当的异步I/O需要该数据库支持异步请求的客户端。许多流行的数据库都提供这样的客户端。

在没有这样的客户端的情况下，可以通过创建多个客户端并使用线程池处理同步调用，尝试将同步客户端转换为有限的并发客户端。然而，这种方法通常比真正的异步IO客户端效率低。

## Async I/O API

Flink的异步API允许用户在数据流中使用异步请求客户端。假设现在你拥有目标数据库的异步客户端，要在数据流上进行异步IO的操作还需要实现如下三个方面：

* 实现`AsyncFunction`接口或继承`RichAsyncFunction`虚拟类，用以发送请求。
* 异步请求回调，用以接收异步IO操作结果，并传递给`ResultFuture`。
* 在数据流上应用异步IO操作。

如下代码展示了异步IO的基本编程模式：

```java
// This example implements the asynchronous request and callback with Futures that have the
// interface of Java 8's futures (which is the same one followed by Flink's Future)

/**
 * An implementation of the 'AsyncFunction' that sends requests and sets the callback.
 */
class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    /** The database specific client that can issue concurrent requests with callbacks */
    private transient DatabaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new DatabaseClient(host, post, credentials);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

        // issue the asynchronous request, receive a future for result
        final Future<String> result = client.query(key);

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }
}

// create the original stream
DataStream<String> stream = ...;

// apply the async I/O transformation
DataStream<Tuple2<String, String>> resultStream =
    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);
```

重要提示：`ResultFuture.complete`方法用于将异步IO的结果填充在`ResultFuture`对象中，该方法在第一次被调用时，`ResultFuture`对象就会完成填充，后续的执行将被忽略。另外，在代码中只能一个地方调用该方法，如果有多次调用，将会导致丢失异步IO的结果。

示例代码中，`    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)`中的1000和100两个参数解释如下：

* Timeout：异步IO操作的超市时间，超过该时间后异步IO操作将视为失败。该参数可以防止对失败的请求无限期等待下去。
* Capacity：此参数定义可以同时处理的异步请求数。尽管异步IO通常会带来更高的吞吐量，但也可能会成为流式应用的瓶颈。限制其并发请求数可以确保异步IO不会累积不断增长的待请求数，但是一旦该容量耗尽，就会触发反压。

### Timeout Handling

若异步IO请求超时，默认会抛出异常，并重启作业。你可以通过重写`AsyncFunction.timeout()`方法来处理这些超时。请确保在重写时调用`ResultFuture.complete()`或`ResultFutere.completeExceptionally()`，以向Flink指示此输入记录的处理已完成。如果不希望在发生超时时发出任何记录，则可以调用`ResultFuture.complete(Collections.emptyList())`。

### Order of Results

`AsyncFunction`发出的异步IO请求通常会以未定义的顺序结束，它不按请求的顺序，还是按照谁先完成的顺序。为了控制请求结果有序返回，Flink提供了两种模式，Unordered和Ordered，而且随着time characteristic的不同，这两种模式的实现也不同：

processing-time：

* Unordered：一旦请求结束，就返回请求结果，这会导致在经过异步IO处理后，数据流中元素的顺序可能会改变。当Flink应用中的time characteristic为processing-time时，该模式具有最少的延迟和开支。你可以通过`AsyncDataStream.unorderedWait(...)`来使用该模式。
* Ordered：这种模式下数据流中元素的顺序在经过异步IO之后会被保留，异步IO前后的元素顺序是一样的。为了做到这一点，在异步IO算子中会缓存返回结果，在其之前的所有返回结果都已经发出时（或超时时），才会继续发出后续的返回结果。与无序模式相比，这通常会导致一些额外的延迟，也会导致checkpoint时更多的支出，因为返回结果可能会在checkpoint状态中保存很长时间。

event-time：

* Unordered：在经过异步IO算子处理后，watermark不会超过数据流中的元素，反之亦然，也就是说watermark形成了一个*order boundary*，两个watermark之间（区间内）的元素可以无序，但是不同的区间内的元素是有序的。在某个watermark之后的元素的返回结果仍在该watermark之后进行输出。当watermark之前的所有元素的返回结果输出后，该watermark才会输出。
    这意味着存在watermark的情况下，无序模式也具有了和有序模式相同的延迟和管理开销，该开销的大小取决于watermark的频率。
* Ordered：在异步IO算子处理前后，watermark和元素的顺序将被保留。与processing-time相比，该模式的开销基本相同。

请记住，*ingestion-time*接入时间是event-time的特例，它可以基于source算子的processing time自动生成watermark。

### Fault Tolerance Guarantees

异步IO算子提供完整的exactly-once容错保证，它将正在发生的异步请求存储在checkpoint中，并在崩溃恢复时恢复或重新触发这些请求。

### Implementation Tips

For implementations with *Futures* that have an *Executor* (or *ExecutionContext* in Scala) for callbacks, we suggests to use a `DirectExecutor`, because the callback typically does minimal work, and a `DirectExecutor` avoids an additional thread-to-thread handover overhead. The callback typically only hands the result to the `ResultFuture`, which adds it to the output buffer. From there, the heavy logic that includes record emission and interaction with the checkpoint bookkeeping happens in a dedicated thread-pool anyways.

A `DirectExecutor` can be obtained via `org.apache.flink.util.concurrent.Executors.directExecutor()` or `com.google.common.util.concurrent.MoreExecutors.directExecutor()`.

### Caveats

`AsyncFunction`并不是多线程

要注意，AsyncFunction不是以多线程的方式调用的。AsyncFunction只有一个实例，并且它是为流的各个分区中的每个记录顺序调用的。除非`asyncInvoke(…)`方法快速返回并依赖于（客户端的）回调，否则它将不会产生正确的异步I/O。

For example, the following patterns result in a blocking `asyncInvoke(...)` functions and thus void the asynchronous behavior:

- Using a database client whose lookup/query method call blocks until the result has been received back
- Blocking/waiting on the future-type objects returned by an asynchronous client inside the `asyncInvoke(...)` method

**An AsyncFunction(AsyncWaitOperator) can be used anywhere in the job graph, except that it cannot be chained to a `SourceFunction`/`SourceStreamTask`.**







