package com.shrbank.bigdata.storm
import java.util

import akka.actor.{ActorPath, ActorRef, ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import com.typesafe.config.ConfigFactory
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{IRichSpout, OutputFieldsDeclarer}
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory


/**
  * Created by wushaojie on 2016/9/8.
  * ActorSpout 用来接收Actor发送的消息，并将消息发送给Top，Top处理完成收发送消息给Spout，该Spout将Top返回的结果转发给源Actor
  */
// 2016.10.10 增加消息产生时间
private[storm] case class SpoutInputMsg(client:ActorPath, message:AnyRef, messageSendTime:String)
private[storm] case class SpoutOutputMsg(client:ActorPath, message:AnyRef)


object SyncSpout{
  private [storm] val systemName = "SpoutServerActorSystem"
  private [storm] val MsgSendTimeFieldName = "MessageTime-9462983c-084c-4966-8f4f-08ec11ca62c6"
  private [storm] val TupleFieldName = "SyncSpoutServer-113fd1d1-bc17-435c-bed0-28029e014aa0"
  private[storm] val ActorConfigFile = "sync-spout-actor.conf"
  private[storm] val ConfigFile = "sync-spout-server.conf"

  lazy private[storm] val config =  ConfigFactory.load(ActorConfigFile).withFallback(ConfigFactory.load(ConfigFile))

  def apply(): SyncSpout = new SyncSpout()
  // SyncSpout组件和SendBolt组件获取消息超时时间，单位毫秒
  private [storm] val MessageTimeThreshold = 1000
  // 每个消息队列容量的默认值
  private [storm] val MessageQueueCapacity = 1024
  private [SyncSpout] var server:ActorSystem = _
  private [SyncSpout] def getActorSystemInstance:ActorSystem = {
    if( server == null ){
      server = ActorSystem.create(SyncSpout.systemName,SyncSpout.config)
    }
    server
  }
}
class SyncSpout extends IRichSpout with IMessageSender{
  private val log = LoggerFactory.getLogger(this.getClass)
  // 输入消息队列
 // private var msgQueue :Array[ISpoutMessageQueue[SpoutInputMsg]] = _
  // 接收消息的actor
  private var msgActor:ActorRef = _
  // 接收消息的actor路径
  private var msgActorPath :String = _
  // SpoutOutputCollector
  private var collector:SpoutOutputCollector = _
  // 当前spout的topology.name
  private var topologyName :String = ""
  // 当前actor端口
  private var serverPort :Int = -1
  // 当前actor网络地址
  private var serverHost:String = _
  // zk配置
  //private var zkConfig:SyncSpoutZkConfig = _
  private var zkConfig:SyncSpoutZkConfig = _
  private var server:ActorSystem = _
  // 该组件初始化时调用，也就是反序列化之后调用。不能序列化的对象都需要在该函数初始化
  // Called when a task for this component is initialized within a worker on the cluster.
  // It provides the spout with the environment in which the spout executes.

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {

    this.topologyName = map.get("topology.name").toString
    log.info(s"SyncSpout[$topologyName] 开始初始化")

   this.zkConfig = new SyncSpoutZkConfig(SyncSpout.config.getString("server.zkServer"),this.topologyName)

    log.debug(s"$topologyName config ${SyncSpout.config.toString}")

    this.server = SyncSpout.getActorSystemInstance
    this.msgActor = server.actorOf( Props.create(classOf[MessageActor],this),name = this.topologyName )
    // 当前system的网路地址信息
    val defaultAddress = server.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    // 获取当前system实际的端口号、IP地址
    this.serverPort = defaultAddress.port.getOrElse(-1)
    this.serverHost = defaultAddress.host.getOrElse("")

    // 拼接remote actor的path
    this.msgActorPath = this.msgActor.path.toStringWithAddress(server.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)
    log.info(s"SyncSpout[$topologyName] remote actor path ${this.msgActorPath} server=${server.hashCode()}")
    this.collector = spoutOutputCollector

    // 将实际的端口号注册到zookeeper中
    zkConfig.registerServerPort(this.serverHost,this.serverPort)
  }
  // Called when an ISpout is going to be shutdown. There is no guarentee that close will be called, because the supervisor kill -9’s
  // worker processes on the cluster.
  // The one context where close is guaranteed to be called is a topology is killed when running Storm in local mode.
  override def close(): Unit = {
    if( null != msgActor ) this.msgActor ! PoisonPill
    if( null != server ) this.server.shutdown()
    if (null != zkConfig ){
      zkConfig.unRegisterServerPort(this.serverHost,this.serverPort)
      zkConfig.deleteServerPath()
      zkConfig.close()
    }
  }

  // 向下一个Bolt发送数据
  override def nextTuple(): Unit = {
  }
  // Called when a spout has been activated out of a deactivated mode. nextTuple will be called on this spout soon.
  // A spout can become activated after having been deactivated when the topology is manipulated using the storm client.
  override def activate(): Unit = {
    // 开始调用nextTuple之前调用，做一些准备工作。因为如果nextTuple没有数据时会休眠一段时间
  }
  // Called when a spout has been deactivated. nextTuple will not be called while a spout is deactivated.
  // The spout may or may not be reactivated in the future.
  override def deactivate(): Unit = {
    // 如果nextTuple没有数据时会休眠一段时间,休眠之前调用该函数
  }

  // The tuple emitted by this spout with the msgId identifier has failed to be fully processed. Typically,
  // an implementation of this method will put that message back on the queue to be replayed at a later time.
  override def fail(o: scala.Any): Unit = {}

  // Storm has determined that the tuple emitted by this spout with the msgId identifier has been fully processed.
  // Typically, an implementation of this method will take that message off the queue and prevent it from being replayed.
  override def ack(o: scala.Any): Unit = {}
  // Declare configuration specific to this component. Only a subset of the “topology.*” configs can be overridden.
  // The component configuration can be further overridden when constructing the topology using TopologyBuilder
  override def getComponentConfiguration: util.Map[String, AnyRef] = null
  // Declare the output schema for all the streams of this topology.
  // declarer - this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(SyncBoltFields(SendBolt.OutputFieldName))
  }
  // MessageActor用来替代nextTuple发送消息的函数
  override def send(input: SpoutInputMsg): Unit = {
    this.collector.emit(new Values(input.message, this.msgActorPath, input.client, input.messageSendTime))
  }
}
