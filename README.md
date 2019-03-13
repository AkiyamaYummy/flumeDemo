# Demo

MNIST解析相关组件。MNIST是深度学习方面十分有名的数据集，该组件在source处解析MNIST中idx3-ubyte格式的文件，将灰度图片的数据作为Event传入channel，在sink处将数据以图片形式存入图片文件。

## DemoPollableSource

Flume Source分为两种，**PollableSource**和**EventDrivenSource**，在这里我借鉴和类比了**Sequence Generator Source**，认为**Sequence Generator Source**与Demo中的需求实现方式相似，**Sequence Generator Source**是**PollableSource**的一种，所以在该Demo中我继承并实现了**AbstractPollableSource**类。

该source顺序解析MNIST数据集文件，将解析出的部分图片数据发送给channel。

表中，必须的属性加粗显示。

| 属性名称     | 默认值 | 描述                                                         |
| ------------ | :----- | ------------------------------------------------------------ |
| **channels** | -      |                                                              |
| **type**     | -      | 需要为 ```top.akiyamamio.flume.source.demo.DemoPollableSource``` |
| **filePath** | -      | 数据集文件路径                                               |
| maxRead      | 100    | 指定最多读取图片的张数                                       |
| batchSize    | 10     | 指定同一批次发送给channel的图片数量                          |
| interval     | 10     | 为了平衡source和sink的数据读写速度 图片发送后线程要进行一段时间的休眠 <br />参数指定了平均每张图片对应的休眠时间 |

## DemoSink 

该sink从channel中读取Event，将图片数据解析为Java ImageIO中的图片对象，然后存为文件，文件以存入时的毫秒数命名。

表中，必须的属性加粗显示。

| 属性名称      | 默认值 | 描述                                                         |
| ------------- | ------ | ------------------------------------------------------------ |
| **channel**   | -      |                                                              |
| **type**      | -      | 需要为```top.akiyamamio.flume.source.demo.DemoSink```        |
| **dirPath**   | -      | 待存入图片的文件夹链接                                       |
| interval      | 1      | 为了防止图片文件重名导致错误的覆盖 该值至少为1               |
| maxBytesToLog | 16     | 写入文件的同时该sink也调用了slf4j中的logger 该参数为输出log而指定 |

## 配置文件示例

```properties
a1.sources=r1
a1.sinks=k1
a1.channels=c1
# Describe/configure the source
a1.sources.r1.type=top.akiyamamio.flume.source.demo.DemoPollableSource
a1.sources.r1.filePath=/root/train-images.idx3-ubyte
a1.sources.r1.batchSize=2
a1.sources.r1.maxRead=10
a1.sources.r1.channels=c1
# Describe the sink
a1.sinks.k1.type= top.akiyamamio.flume.source.demo.DemoSink
a1.sinks.k1.dirPath=/root/demo
# Use a channel which buffers events in memory
a1.channels.c1.type=memory
a1.channels.c1.capacity=1000
a1.channels.c1.transactionCapacity=100 
# Bind the source and sink to the channel
a1.sources.r1.channels=c1
a1.sinks.k1.channel=c1
```

