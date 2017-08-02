# ![logo](https://github.com/shrbank/SyncSpout/blob/master/doc/images/%E4%B8%8A%E6%B5%B7%E5%8D%8E%E7%91%9E%E9%93%B6%E8%A1%8C.png?raw=true)  SyncSpout简介

SyncSpout是上海华瑞银行（SHRB）大数据团队开发的，用来构造可交互的、同步的Storm拓扑的组件。我们在做实时推荐系统中，希望将Storm的并发性和分布式计算能力应用到“请求-响应”范式中，
比如客户的某次购买行为能够以消息的形式发送到storm拓扑中，storm在指定时间返回推荐结果，也就是说storm需要具有可交互性。基于这样的背景，大数据团队开发了SyncSpout组件，
该组件可以接收客户端异步的消息，经过Storm拓扑异步计算，在指定时间内返回给客户端。
## 架构图
![架构图](https://github.com/shrbank/SyncSpout/blob/master/doc/images/SyncSpout.jpg?raw=true)
## 关键组件介绍
* SyncSpout：继承storm的IRichSpout，用于接收客户端调用消息并将消息emit出去的Spout
* SendBolt：拓扑中发送计算结果的bolt，该bolt将计算结果返回给客户端
* SyncSpoutClient：用于向SyncSpout发送同步消息，并在指定时间内获取结果

## 特性
* 使普通的storm应用可交互
* storm应用重启后，客户端可自动重连
* 对storm应用几乎没有侵入，对业务没有侵入
* storm集群返回的计算结果能够准确的返回给指定客户端的某次调用
* 客户端可发送任意类型的消息给storm应用；storm应用可返回任意类型的消息给客户端
* 客户端可在指定时间内同步获取storm应用返回的计算结果
* 支持高并发，在单机环境下1000并发量基本在100毫秒内返回

## 与Storm官方DRPC的异同
* 都能接收一个远程请求，发送请求到storm拓扑，从storm拓扑接收结果，发送结果回等待的客户端
* DRPC只能处理字符串；SyncSpout可以处理任意可序列化的类型
* DRPC仅能处理“线性的”DRPC拓扑，计算以一连串步骤的形式表达；SyncSpout能够处理任意类型的storm拓扑
* DRPC的功能被移植到了Trident中，从原生Storm被废弃了；SyncSpout会被SHRB一直维护

## 用法

### 客户端

    // 创建客户端
    val client = new SyncSpoutClient(topName)
    // 初始化
    client.init()
    // 向远程storm集群发送消息，并在1000毫秒内返回，若超时则返回null指针
    val syncResult = client.ask(ClientMsg("这是发送的消息，可以是任意类型"),1000).asInstanceOf[String]
    println(s"返回消息是[$syncResult]，可以是任意类型")

### storm集群
    val builder = new TopologyBuilder()
    // ActorSpout用于接收消息
    builder.setSpout("syncSpout",SyncSpout(),2)
    // SimpleBolt用于处理消息
    builder.setBolt("simpleBolt",new SimpleBolt(),2).setNumTasks(4).shuffleGrouping("syncSpout")
    // SendBolt用于返回消息
    builder.setBolt("sendBolt",new SendBolt(),2).shuffleGrouping("simpleBolt")
    val cluster = new LocalCluster()
    val topName = "SyncSpoutTop"
    val conf = new Config()
    conf.setNumWorkers(2)
    cluster.submitTopology(topName,conf,builder.createTopology())
    println( "SyncSpout 启动成功!" )

#### 注意点
* 客户端实例化时的topName就是storm集群中的名称
* sync-spout-server.conf、sync-spout-client.conf中需要配置zookeeper的host列表

## 引用第三方类库
* zkclient：https://github.com/yuluows/zkclient.git

## 联系方式
E-MAIL：echo MzY1NzgxMDYyQHFxLmNvbQo= | base64 -d

公司E-MAIL：echo d3VzaGFvamllQHNocmJhbmsuY29tCg== | base64 -d

[![HomeLink](http://ec2-35-160-184-183.us-west-2.compute.amazonaws.com:8080/beacon/gabry/shrbank/github/syncSpout?pixel)](https://github.com/shrbank/SyncSpout)
