Data sinks可以消费数据流，将其中的元素输出到文件、socket、外部系统或打印到控制台。

## Flink Predefined Sinks

本节介绍Flink中内置的Sinks，他们可以在各种算子之后应用在数据流上，这是数据pipeline的最后一环。

* `writeAsText()/TextOutputFormat`，将元素按字符串格式逐行写入文件，该字符串是通过调用元素的`toString()`方法获取的。

* `writeAsCsv(...)/CsvOutputFormat`，将逗号分隔的元组写入文件。行和字段分隔符是可配置的。每个字段的值来自元素字段的`toString()`方法。

* `writeUsingOutputFormat()/FileOutputFormat`，该方法用于实现自定义文件输出，也是文件输出的基类，`writeAsText()`和`writeAsCsv()`都是对该方法的封装。

* `print()/printToErr()`，将每个元素的`toString()`返回值打印到标准输出（stdout）或标准错误流（stderr）。可选的，可在其参数中指定输出的字符串的前缀。如果并行度大于1，在输出中还将添加执行的线程索引，如下`open()`方法。

    ```java
        // PrintSinkOutputWriter<IN>类的open方法
    	public void open(int subtaskIndex, int numParallelSubtasks) {
            this.stream = !this.target ? System.out : System.err;
            this.completedPrefix = this.sinkIdentifier;
            if (numParallelSubtasks > 1) {
                if (!this.completedPrefix.isEmpty()) {
                    this.completedPrefix = this.completedPrefix + ":";
                }
                this.completedPrefix = this.completedPrefix + (subtaskIndex + 1);
            }
    
            if (!this.completedPrefix.isEmpty()) {
                this.completedPrefix = this.completedPrefix + "> ";
            }
        }
    ```

* `writeToSocket`，根据`SerializationSchema`将元素写入套接字。

* `addSink()`，用来调用自定义sink function。Flink通过连接器connector与其他系统绑定在一起（如Apache Kafka），而这些连接器都实现了sink function。

注意，以上write开头的sink是用于内部debug用的，没有实现Flink的checkpoint，意味着通常只支持at-least-once语义。对目标系统的数据刷新取决于OutputFormat的实现。这意味着并非所有发送到OutputFormat的元素都会立即显示在目标系统中。此外，在失败的情况下，这些记录可能会丢失。

为了可靠地将流准确地传递到文件系统中，请使用`StreamingFileSink`。此外，通过`addSink()`方法的自定义实现可以参与Flink的checkpoint，实现exactly-once语义。





​    

