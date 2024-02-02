## Overview

Flink中支持两种join：Window Join 和 Interval Join。

## Window Join

> 对于两条流的合并，很多情况我们并不是简单地将所有数据放在一起，而是希望根据某个字段的值将它们联结起来，“配对”去做处理。例如用传感器监控火情时，我们需要将大量温度传感器和烟雾传感器采集到的信息，按照传感器ID分组、再将两条流中数据合并起来，如果同时超过设定阈值就要报警。我们发现，这种需求与关系型数据库中表的join操作非常相近。事实上，Flink中两条流的connect操作，就可以通过keyBy指定键进行分组后合并，实现了类似于SQL中的join操作；另外connect支持处理函数processfunction，可以使用自定义状态和TimerService灵活实现各种需求，其实已经能够处理双流合并的大多数场景。基于时间的操作，最基本的当然就是时间窗口了，时间窗口主要是针对单一数据流在某些时间段内的处理计算。那如果我们希望将两条流的数据进行合并、且同样针对某段时间进行处理和统计，Flink为这种场景专门提供了一个窗口关联（window join）算子，可以定义时间窗口，并将两条流中共享一个key的数据放在窗口中进行配对处理。
>
> 参考自：https://www.cnblogs.com/wdh01/p/16650173.html

窗口关联（Window Join）把两个数据流中，位于同一个窗口中且具有相同key的元素进行关联。这些窗口可以由窗口分配器进行定义，并会处理两条流中的元素。
窗口关联在代码中的实现，首先需要调用`DataStream.join()`方法来合并两条流，得到一个`JoinedStreams`；接着通过`.where()`和`.equalTo()`方法指定两条流中联结的key；然后通过`.window()`开窗口，并调用`.apply()`传入联结窗口函数进行处理计算。通用调用形式如下：

```java
stream.join(otherStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(<WindowAssigner>)
    .apply(<JoinFunction>);
```

上述代码结构中，`where()`方法指定第一条流中的key，`equalTo()`方法指定第二条流中的key。两者相同的元素，如果在同一窗口中，就可以匹配起来，并通过一个“关联函数”（JoinFunction）进行处理了。这里`window()`传入的就是窗口分配器。而后面调用`apply()`可以看作实现了一个特殊的窗口函数。注意这里只能调用`apply()`，没有其他替代的方法。传入的`JoinFunction`也是一个函数类接口，使用时需要实现内部的`join()`方法。这个方法有两个参数，分别表示两条流中成对匹配的数据。JoinFunction在源码中的定义如下：

```java
public interface JoinFunction<IN1, IN2, OUT> extends Function, Serializable {
    OUT join(IN1 var1, IN2 var2) throws Exception;
}
```

这里需要注意，JoinFunciton并不是真正的“窗口函数”，它只是定义了窗口函数在调用时对匹配数据的具体处理逻辑。当然，既然是窗口计算，在`window()`和`apply()`之间也可以调用可选API去做一些自定义，比如用`trigger()`定义触发器，用`allowedLateness()`定义允许延迟时间，等等。

关于join语义的一些注意事项：

* join是对两条数据流中的元素进行成对的匹配，在语义上与SQL中的inner-join类似。如果一条流中的元素在另一条数据流中没有与其匹配的元素，则不会输出任何结果。
* 这些参与join的元素都有其自己的时间戳，该时间戳取自这些元素所属窗口的允许的最大时间戳，即`TimeWindow.getEnd()-1`或者`TimeWindow.maxTimestamp()`。例如，时间范围为[5, 10)的时间窗口，其中参与join的元素的时间戳都是9。

### Tumbling Window Join

> JoinFunction中的两个参数，分别代表了两条流中的匹配的数据。这里就会有一个问题：什么时候就会匹配好数据，调用.join()方法呢？接下来我们就来介绍一下窗口join的具体处理流程。两条流的数据到来之后，首先会按照key分组、进入对应的窗口中存储；当到达窗口结束时间时，算子会先统计出窗口内两条流的数据的所有组合，也就是对两条流中的数据做一个笛卡尔积（相当于表的交叉连接，cross join），然后进行遍历，把每一对匹配的数据，作为参数(first，second)传入`JoinFunction`的`.join()`方法进行计算处理。所以窗口中每有一对数据成功联结匹配，`JoinFunction.join()`方法就会被调用一次，并输出一个结果。

对于滚动窗口关联（Tumbling Window Join），把两个数据流中，位于同一个窗口中且具有相同key的元素成对匹配，并发送到apply算子中的`JoinFunction`或`FlatJoinFunction`函数中计算。该操作类似于关系数据库中的inner join，所以如果一条流中的元素在另一条数据流中没有与其匹配的元素，则不会输出任何结果。

如下图所示，我们定义了一个`TumblingEventTimeWindows.of(Time.milliseconds(5))`的窗口，所以窗口的时间范围为[0,1], [2,3], ...。图中可以看到，每个窗口中，两条流中的所有数据会两两组合（笛卡尔积），然后传递给`JoinFunction`（若是指定了关联条件，则符合关联条件的元素才会传递给`JoinFunction`）。注意在[6,7]窗口中，第一条流中没有任何数据，所以第二条数据中的元素6和7不会与任何元素进行关联，所以该窗口没有输出。

![img](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/tumbling-window-join-6282760.svg)

示例代码：

```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
 
...

DataStream<Integer> orangeStream = ...;
DataStream<Integer> greenStream = ...;

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```

### Sliding Window Join

对于滑动窗口关联（Sliding Window Join），与滚动窗口关联类似，同一个滑动窗口中具有相同key的元素会关联为成对的组合，并传递给`JoinFunction`或`FlatJoinFunction`。若在当前窗口中，一条流中的元素与另一条流中的元素无法匹配，则不会输出任何结果。注意，与滚动窗口不同的是，在某个滚动窗口中没有关联起来的元素，可能会在下一个窗口中关联起来。如下图所示：
![img](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/sliding-window-join-6508445.svg)

在上图的例子中，我们定义了滑动窗口的大小为2毫秒，步长为1毫秒，所以会创建这些窗口序列：[-1, 0], [0, 1], [1, 2], [2, 3]...。其中X轴以下的元素对都将被传递到`JoinFunction`中。图中我们可以看到，橙色元素2与绿色元素3是在[2, 3]窗口中关联起来的，而不是在[1, 2]窗口中。

```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

...

DataStream<Integer> orangeStream = ...;
DataStream<Integer> greenStream = ...;

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(SlidingEventTimeWindows.of(Time.milliseconds(2) /* size */, Time.milliseconds(1) /* slide */))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```

### Session Window Join

对于会话窗口关联（Session Window Join），所有在同一个会话窗口中且具有相同key的元素会关联成成对的组合，并传递给`JoinFunction`或`FlatJoinFunction`。仍然类似于inner join，若一个会话窗口中只有一条数据流的元素，则不会输出任何结果。

![img](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/session-window-join.svg)

这里我们定义了一个gap为1毫秒的会话窗口，可以看到一共有三个窗口实例，其中前两个都成功关联到了，第三个窗口则未关联成功。

```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
 
...

DataStream<Integer> orangeStream = ...;
DataStream<Integer> greenStream = ...;

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```

## Interval Join

区间关联（Interval Join）将两条数据流中（A流与B流）具有相同key的元素进行关联，并且数据流B中元素的时间戳位于数据流A中元素时间戳的相对时间范围内，通过表达式更容易理解：`b.timestamp ∈ [a.timestamp + lowerBound; a.timestamp + upperBound]`或者是`a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound`。

其中，upperBound要小于等于upperBound，上下限既可以是正数，也可以是负数。默认情况下，该区间是闭区间，但是可以调用`.lowerBoundExclusive()`和`.upperBoundExclusive()` 改为开区间。

当一对元素被传递给`ProcessJoinFunction`时，它们将被分配两个元素中较大的时间戳（可以通过`ProcessJoinFunction.Context.getTimestamp()`访问）。

目前区间关联只支持inner join语义，只支持event-time。

![img](/Users/wcp/DBA-Database/My-Obsidian/markdown图片/interval-join.svg)

上面的例子中，我们将橙色的数据流和绿色的数据流进行关联，下限为-2毫秒，上限为+1毫秒，即`orangeElem.ts + (-2ms) <= greenElem.ts <= orangeElem.ts + (1ms)`。以下是这个例子的代码实现：
```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

...

DataStream<Integer> orangeStream = ...;
DataStream<Integer> greenStream = ...;

orangeStream
    .keyBy(<KeySelector>)
    .intervalJoin(greenStream.keyBy(<KeySelector>))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process (new ProcessJoinFunction<Integer, Integer, String(){

        @Override
        public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
            out.collect(left + "," + right);
        }
    });
```



