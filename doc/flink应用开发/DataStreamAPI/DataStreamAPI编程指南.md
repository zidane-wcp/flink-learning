Flink 中的 DataStream 程序，可以实现数据流转换（例如，过滤、更新状态、定义窗口、聚合）。

数据流最初是从各种source（例如消息队列、套接字流、文件）创建的。数据流在创建后，可以经过各种 transformation，将结果发送给 sink，sink 可以将其写入各种外部系统，例如可以将数据写入文件或标准输出（例如命令行终端）。简单说，就是数据流会经过source、transformation、sink三个过程。后面的章节将通过具体的代码展示这个过程。

Flink 程序可以在各种环境中运行，可以独立运行，也可以嵌入其他程序中。执行可以发生在本地 JVM 中，也可以发生在许多机器的集群上。

## 什么是DataStream？

DataStream API 的名称来自于一个特殊的 DataStream 类，该类用于表示 Flink 程序中的数据集合。可以将它们视为可以包含重复项的不可变数据集合。这些数据可以是有限的，也可以是无界的，但是可以用同一套API来处理他们，这就是流批一体。

DataStream在某些方面的用法与Java中的集合很相似，但在某些关键方面有很大不同。DataStream是不可变的，这意味着一旦创建它们，就无法添加或删除元素。也不能简单地操作内部元素，而只能使用DataStream API 操作（也称为转换）对其进行处理。

可以通过在 Flink 程序中添加source来创建 DataStream。然后可以从中派生新的流，并通过类似于map、filter的方法去组合并处理这些流。

## Flink程序剖析

Flink程序是用来操作DataStream的，包括创建、转换、输出。每个Flink程序都包含以下几个基本的部分：

1. 获取execution environment。
2. 创建数据源。
3. 对数据源进行转换（transformation）操作。
4. 指定转换结果的输出位置。
5. 触发程序的执行。

