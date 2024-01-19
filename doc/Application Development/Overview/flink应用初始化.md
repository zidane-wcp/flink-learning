

Flink应用程序的项目初始化目前支持三种方式，包括Maven、Gradle、Flink官方提供的项目初始化脚本。

## Maven

**Interactive模式**

```bash
mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.15.4
```

**Batch模式**

```bash
mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.15.4 \
  -DgroupId=org.wcp.flink \
  -DartifactId=function\
  -Dversion=0.1 \
  -Dpackage=org.wcp.flink \
  -DinteractiveMode=false
```

## 初始化脚本

```bash
curl https://flink.apache.org/q/quickstart.sh | bash -s 1.15.4
```

## Gradle

## 管理项目依赖

当开发Flink作业时，通常需要以下几种依赖：

* Flink API，用于开发你的Flink作业。
* Connectors and formats，用于Flink作业与外部系统的交互，比如Flink CDC、flink-connector-clickhouse等。
* 测试工具，用于测试你的Flink作业。

关于Flink API，Flink主要提供了两种API：Datastream API和Table API && SQL。这两种API可以分开来单独使用，也可以混合使用，取决于你的使用场景。下表展示了每种API需要的依赖包。

| APIs you want to use                                         | Dependency you need to add          |
| ------------------------------------------------------------ | ----------------------------------- |
| [DataStream](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/overview/) | `flink-streaming-java`              |
| [DataStream with Scala](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/scala_api_extensions/) | `flink-streaming-scala_2.12`        |
| [Table API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/common/) | `flink-table-api-java`              |
| [Table API with Scala](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/common/) | `flink-table-api-scala_2.12`        |
| [Table API + DataStream](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/data_stream_api/) | `flink-table-api-java-bridge`       |
| [Table API + DataStream with Scala](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/data_stream_api/) | `flink-table-api-scala-bridge_2.12` |



## IDE中运行你的Flink应用

当你成功对Flink作业进行初始化之后，我们建议将项目导入IDE中进行开发和测试。

在 IDE 中运行项目可能会导致异常`java.lang.NoClassDefFoundError`，这可能是因为没有将所有必需的 Flink 依赖项隐式加载到类路径中。IntelliJ IDEA的修复方式如下：
Run > Edit Configurations > Modify options > Select `include dependencies with "Provided" scope`。

## 打包项目

当你想打包你的项目时，你可以到项目的根目录下，执行`mvn clean package`，你将在`target`目录下找到一个名为``target/<artifact-id>-<version>.jar`.`的Jar文件，该文件就是你的Flink应用的Jar包，里面包含了你的作业代码、connector、以及其他你添加的依赖。

注意：当你的Flink应用的main class不是`DataStreamJob`时，若将这个Jar包提交到Flink集群去执行，可能会报找不到main class的异常。你可以在`pom.xml`文件中，通过设置`mainClass`选项来解决。