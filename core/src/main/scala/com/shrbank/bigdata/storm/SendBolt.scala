package com.shrbank.bigdata.storm

import java.util

import akka.actor.{ActorPath, ActorSystem, ExtendedActorSystem}
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

/**
  * Created by wushaojie on 2016/9/8.
  * 最后处理的结果都需要通过该Bolt发送，发送给客户端的消息放在Message字段
  */
object SendBolt {
  /***
    * SendBolt对应的ActorSystem名称
    */
  private val systemName = "SendBoltActorSystem"
  /**
    * SendBolt消息的字段名
    */
  val OutputFieldName = "SendBoltMessage-0f284c46-bf8c-4eed-9111-b87e8ae5196f"
}
class SendBolt extends BaseBasicBolt{
  private val log = LoggerFactory.getLogger(this.getClass)
  /**
    * SendBolt是否初始化
    */
  private var isPrepare = false
  /**
    * SendBolt对应的ActorSystem占用的端口号
    */
  private var sendBoltPort = -1
  /**
    * SendBolt对应的ActorSystem占用的host
    */
  private var sendBoltHost = ""
  /**
    * SendBolt对应的ActorSystem实例
    */
  private var system:ActorSystem = _
  /**
    * SyncSpout在zk中的配置
    */
  private var zkConfig:NewSyncSpoutZkConfig = _

  /**
    * SendBolt退出时清理资源
    */
  override def cleanup(): Unit = {
    system.shutdown()
    zkConfig.unRegisterClientPort(this.sendBoltHost,this.sendBoltPort)
    isPrepare = false
  }

  /**
    * SendBolt初始化：创建ActorSystem
    * @param stormConf 提交时的stormConf
    * @param context 当前的TopologyContext
    */
  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    if(!this.isPrepare){
      val topologyName = stormConf.get("topology.name").toString
      log.info(s"SendBolt[$topologyName] 开始初始化")

      this.zkConfig = new NewSyncSpoutZkConfig(SyncSpout.config.getString("server.zkServer"),topologyName)

      log.debug(s"SendBolt[$topologyName] config ${SyncSpout.config}")

      system =  ActorSystem.create(SendBolt.systemName,SyncSpout.config)
      // 当前system的网路地址信息
      val defaultAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
      // 获取当前system实际的端口号、IP地址
      sendBoltPort = defaultAddress.port.getOrElse(-1)
      sendBoltHost = defaultAddress.host.getOrElse("")
      zkConfig.registerClientPort(sendBoltHost,sendBoltPort)
      log.info(s"SendBolt[$topologyName] 占用端口 $sendBoltHost:$sendBoltPort")
      this.isPrepare = true
    }

  }

  /**
    * SendBolt的处理逻辑：将处理后的消息发送给SyncSpout
    * @param tuple 处理后的数据tuple
    * @param basicOutputCollector outputCollector
    */
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    //val server = tuple.getValueByField(SyncSpout.TupleFieldName).asInstanceOf[String]
    val client = tuple.getValueByField(SyncSpoutClient.TupleFieldName).asInstanceOf[ActorPath]
    val msg = tuple.getValueByField(SendBolt.OutputFieldName)
    val msgSendTime = tuple.getValueByField(SyncSpout.MsgSendTimeFieldName).asInstanceOf[String]
    if( (System.currentTimeMillis - msgSendTime.toLong) > SyncSpout.MessageTimeThreshold )
      log.warn(s"返回该消息$msg, 耗时 ${System.currentTimeMillis - msgSendTime.toLong} 毫秒")

    // system.actorSelection(server) !  SpoutOutputMsg(client, msg)
    // 直接返回消息给client，以减少SyncSpout的压力
    system.actorSelection(client) !  msg

    basicOutputCollector.emit(SyncBoltValues(tuple,msg))
  }

  /**
    * 为emit的数据定义schema，向后面bolt传送的数据在SendBolt.OutputFieldName字段中
    * @param outputFieldsDeclarer outputFieldsDeclarer
    */
  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(SyncBoltFields(SendBolt.OutputFieldName))
  }
}