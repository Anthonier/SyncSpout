package com.shrbank.bigdata.examples

import com.shrbank.bigdata.storm.{ClientMsg, SyncSpoutClient}

/**
  * Created by wushaojie on 2016/9/8.
  * SyncSpoutClient使用示例
  */
object SyncSpoutClientDemo {
  def main(args: Array[String]): Unit = {
    val topName = "SyncSpoutTop"
    println(s"连接远程topologyName是${topName}的server")
    val client = new SyncSpoutClient(topName)
    val msgNum = 10
    if(client.init()){
      println("初始化成功，开始发送消息")
      val startTime = System.nanoTime
      // 1000毫秒内返回消息
      for(i<-0 to msgNum){
        val syncResult = client.ask(ClientMsg("客户端消息"),1000).message.asInstanceOf[String]
        println(s"返回消息是[$syncResult]")
      }
      val endTime = System.nanoTime
      println(s"发送 $msgNum 条消息，耗时 ${endTime-startTime} 纳秒，${(endTime-startTime)/1000000} 毫秒，平均耗时 ${(endTime-startTime)/msgNum/1000000} 毫秒")
      // Thread.sleep(300*1000)
    }else{
      println(s"SyncSpoutClient 初始化失败，请检测远程storm是否启动")
      // 用以验证远程storm重启后，客户端可以重连发送消息
      Thread.sleep(300*1000)
      for(i<-0 to msgNum){
        val syncResult = client.ask(ClientMsg("客户端消息"),1000).message.asInstanceOf[String]
        println(s"返回消息是[$syncResult]")
      }
    }
    client.destroy()
  }
}
