package com.shrbank.bigdata.storm

import akka.actor.{ActorIdentity, ActorRef, ActorSystem, Identify}
import akka.pattern.{AskableActorSelection, Patterns}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._

/**
  * Created by wushaojie on 2016/9/9.
  * SyncSpoutClient用来与SyncSpout通信的客户端
  */
object SyncSpoutClient {
  private val systemName = "SpoutClientActorSystem"

  private[storm] val TupleFieldName ="SyncSpoutClient-acece387-7154-454c-9a61-e7a6595db760"
  /**
    * SyncSpoutClient配置文件名
    */
  private[storm] val ConfigFile = "sync-spout-client.conf"

  lazy private[SyncSpoutClient] val config = ConfigFactory.load(SyncSpout.ActorConfigFile).withFallback(ConfigFactory.load(ConfigFile))

  lazy private[SyncSpoutClient] val client = ActorSystem.create(systemName,config)
  /**
    * 初始化时用来判断远程SyncSpout是否可用的超时时间
    */
  private val initTimeOut = new Timeout(config.getInt("client.init.timeout"),MILLISECONDS)
  /**
    * 发送同步消息时的超时时间
    */
  private val askTimeOut = new Timeout(config.getInt("client.ask.timeout"),MILLISECONDS)
}
class SyncSpoutClient{
  private val log = LoggerFactory.getLogger(this.getClass)
  // 客户端对应的拓扑名
  private var topologyName:String = _
  // 对应topologyName所有可用的服务器列表，用来负载均衡
  private val serverRefs = new scala.collection.mutable.HashMap[(String,Int),ActorRef]()
  private def getServerRefArray = this.serverRefs.toArray
  // 当前使用的server索引，每次ask后，简单的+1
  private var currentServerIndex = -1
  private var zkConfig:SyncSpoutZkConfig = _
 
  def this(topologyName:String) {
    this()
    this.topologyName = topologyName
    zkConfig = new SyncSpoutZkConfig(SyncSpoutClient.config.getString("client.zkServer"),topologyName)
    start()
    zkConfig.watchServer(start)
  }

  private def start() :Unit ={
    this.currentServerIndex = 0
    val servers = zkConfig.getServerPort
    if( servers.nonEmpty ){
      servers.foreach{ hostAndPort =>
        connect(hostAndPort._1,hostAndPort._2)
      }
    }else{
      log.warn("server is empty!")
      this.serverRefs.clear()
    }
  }
  /**
    * 启动与目标服务器的连接
    * @param host 目标服务器的IP
    * @param port 目标服务器的端口
    */
  private def connect(host:String,port:Int):Unit = {
    val serverPath =  s"akka.tcp://${SyncSpout.systemName}@$host:$port/user/$topologyName"
    val serverActorSelection = SyncSpoutClient.client.actorSelection(serverPath)
    // 在指定时间内查找指定的ActorRef
    val actorAsker = new AskableActorSelection(serverActorSelection)
    val actorRefFuture = actorAsker.ask(Identify(None))(SyncSpoutClient.initTimeOut)
    try{
      log.info(s"尝试连接$host-$port")
      val workerActorRef = Await.result(actorRefFuture,SyncSpoutClient.initTimeOut.duration).asInstanceOf[ActorIdentity].getRef
      // 防止初始化多次
      if( workerActorRef != null ){
        this.serverRefs += (host,port)->workerActorRef
        log.info(s"与远程SyncSpout[$host:$port/$topologyName]建立连接,serverRefs=${this.serverRefs.values.mkString(",")}")
      }else{
        log.warn(s"$host-$port,连接失败")
        disconnect(host,port)
      }
    }catch {
      case timeEx:TimeoutException =>
        log.error(s"与[$host:$port/$topologyName]连接超时")
      case ex:Exception=>
        log.error(ex.getMessage,ex)
    }
  }

  /**
    * 断开与目标服务器的连接，将其从列表中删除
    * @param host 目标服务器的IP
    * @param port 目标服务器的端口
    */
  private def disconnect(host:String,port:Int):Unit = {
    log.info(s"disconnect from $host,$port")
    this.serverRefs -= ((host,port))
  }
  /**
    * 从zk中选取一个可用的SyncSpout端口，只调用一次，后续重启通过autoRestart函数自动完成
    * @return 初始化成功返回true；否则返回false
    */
  def init():Boolean = this.serverRefs.nonEmpty
  /**
    * 获取下一个可用的SyncSpout-Server，做简单的负载均衡
    * @return 下一个可用的SyncSpout-Server
    */
  private def nextServer():Option[ActorRef] = {
    if(this.serverRefs.nonEmpty){
      this.currentServerIndex = ( this.currentServerIndex + 1 ) % this.serverRefs.size
      val nextServer = getServerRefArray(this.currentServerIndex)._2
      Some(nextServer)
    }else None
  }
  /**
    * 向server发送消息，并在指定时间内返回
    * @param msg 发送给SyncSpout的消息
    * @param timeout 超时时间
    * @return 若在timeout之间内，SyncSpout有返回，则返回storm计算的结果，否则返回null
    */
  private def ask(msg:ClientMsg,timeout:Timeout):AnyRef = {
    var returnMsg :AnyRef = null
    try{
      nextServer() match {
        case Some(server) =>
          val future = Patterns.ask(server,msg,timeout)
          returnMsg = Await.result(future,timeout.duration)
        case None=>
          log.warn(s"server size is null ,${this.serverRefs.size}")
      }
    }catch {
      case timeEx:TimeoutException =>
      case ex:Exception =>
        log.error(s"currentServerIndex=${this.currentServerIndex},serverRefs=${this.serverRefs},size=${this.serverRefs.size}",ex)
    }
    log.debug(s"处理消息$msg，耗时${System.currentTimeMillis() - msg.msgSendTime} 毫秒")
    returnMsg
  }
  /**
    * 向server发送消息，并在指定时间内返回
    * @param msg 发送给SyncSpout的消息
    * @param timeout 超时时间，单位是毫秒
    * @return 若在timeout之间内，SyncSpout有返回，则返回storm计算的结果，否则返回null
    */
  def ask(msg:ClientMsg,timeout:Long):AnyRef = {
    val askTimeOut = new Timeout(timeout,MILLISECONDS)
    ask(msg,askTimeOut)
  }
  /**
    * 向server发送消息，并在默认时间内返回
    * @param msg 发送给SyncSpout的消息
    * @return 若在timeout之间内，SyncSpout有返回，则返回storm计算的结果，否则返回null
    */
  def ask(msg:ClientMsg) :AnyRef = {
    ask(msg,SyncSpoutClient.askTimeOut)
  }
  /**
    * 释放客户端资源，并退出
    */
  def destroy() = {
    serverRefs.clear()
    currentServerIndex = 0
    SyncSpoutClient.client.shutdown()
    this.zkConfig.close()
  }
}
