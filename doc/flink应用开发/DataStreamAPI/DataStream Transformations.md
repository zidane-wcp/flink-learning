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



















