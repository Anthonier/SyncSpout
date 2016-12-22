package com.shrbank.bigdata.storm

import java.io.IOException
import java.net.{InetAddress, Socket}

/**
  * Created by wushaojie on 2016/9/8.
  * 工具类
  */
object utils {
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
  def isPortUsing(host:String,port:Int) = {
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
  def getValidPort( minPort:Int,maxPort:Int ) = {
    var validPort = -1
   for( port <- minPort to maxPort if validPort == -1){
     if(isPortUsing(localIp,port)) validPort = port
    }
    validPort
  }
}