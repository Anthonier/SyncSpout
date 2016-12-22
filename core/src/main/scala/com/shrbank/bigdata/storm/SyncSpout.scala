package com.shrbank.bigdata.storm
import java.util

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import com.shrbank.bigdata.storm.queue._
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{IRichSpout, OutputFieldsDeclarer}
import com.typesafe.config.ConfigFactory
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory


/**
  * Created by wushaojie on 2016/9/8.
  * ActorSpout 用来接收Actor发送的消息，并将消息发送给Top，Top处理完成收发送消息给Spout，该Spout将Top返回的结果转发给源Actor
  */
// 2016.10.10 增加消息产生时间
private[storm] case class SpoutInputMsg(client:ActorPath, message:AnyRef, messageSendTime:String)
private[storm] case class SpoutOutputMsg(client:ActorPath, message:AnyRef)

/**
  * 消息转发的actor
  * 该actor用来接收client发送的消息，并将storm计算后的数据返回给调用client
  */
private[storm] class MessageActor extends Actor with ActorLogging{

  // 消息转发的队列
  private var msgQueue:Array[ISpoutMessageQueue[SpoutInputMsg]] = _
  private var msgQueueSize = 0
  def this( msgQueue:Array[ISpoutMessageQueue[SpoutInputMsg]]) = {
    this()
    this.msgQueue = msgQueue
    this.msgQueueSize = msgQueue.length
  }
  override def receive: Receive = {
    /**
      * 2016年10月10日 10:03:54 增加消息产生时间的处理
      */
    case ClientMsg(msg,msgSendTime) =>
      val currentThread = Thread.currentThread
      val dispatcherId = currentThread.getId.toInt
      if( (System.currentTimeMillis - msgSendTime.toLong) > SyncSpout.MessageTimeThreshold )
        log.warning(s"收到客户端消息 $msg，耗时${ System.currentTimeMillis - msgSendTime.toLong } 毫秒")
      log.debug(s"收到客户端消息 $msg，耗时${ System.currentTimeMillis - msgSendTime.toLong } 毫秒")
      // 将消息发送给Spout的消息队列
      var addSuc = false
      for( i<- 0 until msgQueueSize if !addSuc ){
        // 防止index溢出，currentThread.getId.toInt是溢出的原因
        val index = (dispatcherId + i)%msgQueueSize
        addSuc = msgQueue( if(index>0) index else -index ).add(SpoutInputMsg(this.sender().path,msg,msgSendTime.toString))
      }
      if(!addSuc)
        log.warning(s"队列满了，消息接收失败,$msg,$msgSendTime")
    case SpoutOutputMsg(client,msg) =>
      log.debug(s"收到返回消息 $msg")
      if(client.name !="deadLetters")
        this.context.actorSelection(client) !  msg
    case unKnownMsg =>
      log.error(s"非法消息 $unKnownMsg")
  }
}
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
}
class SyncSpout extends IRichSpout {
  private val log = LoggerFactory.getLogger(this.getClass)
  // 标志该SyncSpout在当前jvm中是否已经open过
  private var isOpen = false
  // 输入消息队列
  private var msgQueue :Array[ISpoutMessageQueue[SpoutInputMsg]] = _
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
  private var zkConfig:NewSyncSpoutZkConfig = _
  private var server:ActorSystem = _
  // 该组件初始化时调用，也就是反序列化之后调用。不能序列化的对象都需要在该函数初始化
  // Called when a task for this component is initialized within a worker on the cluster.
  // It provides the spout with the environment in which the spout executes.

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    if(!this.isOpen){
      this.topologyName = map.get("topology.name").toString
      log.info(s"SyncSpout[$topologyName] 开始初始化")
      val capacity = try {
        SyncSpout.config.getInt("server.msg.queue.capacity")
      }catch {
        case e:Exception=>
          SyncSpout.MessageQueueCapacity
      }
      // 固定三个MPSC队列
      this.msgQueue = Array(new MultiProducerSingleConsumerQueue[SpoutInputMsg](capacity),new MultiProducerSingleConsumerQueue[SpoutInputMsg](capacity),new MultiProducerSingleConsumerQueue[SpoutInputMsg](capacity))
      this.zkConfig = new NewSyncSpoutZkConfig(SyncSpout.config.getString("server.zkServer"),this.topologyName)

      log.debug(s"$topologyName config ${SyncSpout.config.toString}")

      this.server = ActorSystem.create(SyncSpout.systemName,SyncSpout.config)
      this.msgActor = server.actorOf( Props.create(classOf[MessageActor],msgQueue),name = this.topologyName )
      // 当前system的网路地址信息
      val defaultAddress = server.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
      // 获取当前system实际的端口号、IP地址
      this.serverPort = defaultAddress.port.getOrElse(-1)
      this.serverHost = defaultAddress.host.getOrElse("")

      // 拼接remote actor的path
      this.msgActorPath = this.msgActor.path.toStringWithAddress(server.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress)
      log.info(s"SyncSpout[$topologyName] remote actor path ${this.msgActorPath}")
      this.collector = spoutOutputCollector
      this.isOpen = true

      // 将实际的端口号注册到zookeeper中
      zkConfig.registerServerPort(this.serverHost,this.serverPort)
    }else
      log.warn("该spout实例重复打开？")
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
    // 每次只emit一条数据，以尽快返回，防止nextTuple阻塞时间过长
    msgQueue.foreach { queue =>
      val msg = queue.poll()
      if (msg != null) {
        this.collector.emit(new Values(msg.message, this.msgActorPath, msg.client, msg.messageSendTime))
      }
    }
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
}
