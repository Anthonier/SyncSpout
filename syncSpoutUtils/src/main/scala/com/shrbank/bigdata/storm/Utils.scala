package com.shrbank.bigdata.storm

import java.io.IOException
import java.net.{InetAddress, Socket}

/**
  * Created by ShaoJie.Wu on 2017/4/12 10:09.
  * 功能：
  *
  * 备注：
  */
object Utils {
  /**
    * SyncSpoutClient配置文件名
    */
  private[storm] val ClientConfigFile = "sync-spout-client.conf"
  private[storm] val ServerConfigFile = "sync-spout-server.conf"
  private[storm] val ActorConfigFile = "sync-spout-actor.conf"
  private[storm] val ServerSystemName = "SpoutServerActorSystem"
  private[storm] val ClientSystemName = "SpoutClientActorSystem"
  private[storm] val StormConfig = org.apache.storm.utils.Utils.readStormConfig()
  /***
    * SendBolt对应的ActorSystem名称
    */
  private[storm] val SendBoltSystemName = "SendBoltActorSystem"
  // SyncSpout组件和SendBolt组件获取消息超时时间，单位毫秒
  private [storm] val MessageTimeThreshold = 1000
  /**
    * SendBolt消息的字段名
    */
  val SendBoltOutputFieldName = "SendBoltMessage-0f284c46-bf8c-4eed-9111-b87e8ae5196f"
  private[storm] val ClientTupleFieldName = "SyncSpoutClient-acece387-7154-454c-9a61-e7a6595db760"
  private[storm] val SpoutTupleFieldName = "SyncSpoutServer-113fd1d1-bc17-435c-bed0-28029e014aa0"
  private[storm] val MsgSendTimeFieldName = "MessageTime-9462983c-084c-4966-8f4f-08ec11ca62c6"
  // 本地IP地址
  val localIp:String = {
    val addrs = InetAddress.getAllByName(InetAddress.getLocalHost.getHostName)
    if( addrs.length > 1 ){
      val possibleIp = addrs.filter(_.isSiteLocalAddress).filter(!_.getHostAddress.startsWith("192.168"))
      if(possibleIp.length>1) possibleIp(0).getHostAddress
      else if( possibleIp.length == 1 ) possibleIp(0).getHostAddress
      else "127.0.0.1"
    }else if( addrs.length == 1 ) addrs.head.getHostAddress
    else "127.0.0.1"
  }

  /**
    * 检测端口是否被占用，需要完善代码
    * @param host 待检测的IP
    * @param port 待检测的端口号
    * @return 若被占用则返回false；否则返回true
    */
  def isPortUsing(host:String,port:Int):Boolean = {
    var isUsing = false
    try{
      val socket = new Socket(host,port)
      isUsing = true
      socket.close()
    }catch {
      case e: IOException =>
    }
    isUsing
  }
  /**
    * 在指定范围内查询可用端口号
    * @param minPort 起始端口号
    * @param maxPort 终止端口号
    * @return 可用端口号
    */
  def getValidPort( minPort:Int,maxPort:Int ):Int = {
    var validPort = -1
    for( port <- minPort to maxPort if validPort == -1){
      if(isPortUsing(localIp,port)) validPort = port
    }
    validPort
  }
}
