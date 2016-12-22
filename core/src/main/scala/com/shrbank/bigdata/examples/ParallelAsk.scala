package com.shrbank.bigdata.examples

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.shrbank.bigdata.storm.{ClientMsg, SyncSpoutClient}


/**
  * Created by wushaojie on 2016/9/22.
  * 创建并发线程，调用ask函数，验证结果正确性和性能
  */
class ParallelAskThread(threadName:String) extends Runnable{

  override def run(): Unit = {
    val startTime = System.currentTimeMillis()
    val msg = threadName +" "+ startTime
    val result = ParallelAsk.client.ask(ClientMsg(msg),1000)
    val endTime = System.currentTimeMillis
    // 返回结果为空则代表超时
    if(result == null)
      println(s"current thread  $threadName,exception,result=$result,,${endTime-startTime} ms")
  }
}
object ParallelAsk {
  val client = new SyncSpoutClient("SyncSpoutTop")
  def main(args: Array[String]): Unit = {
    val Array(threadNum,sendNum) = args
    if(client.init()){
      println(s"begin send msg parallel $threadNum，send num $sendNum")
      val threadPool = Executors.newFixedThreadPool(threadNum.toInt)
      try{
        for(i<-0 until sendNum.toInt){
          for (j<- 0 until threadNum.toInt){
            val thread = new ParallelAskThread(UUID.randomUUID().toString)
            threadPool.execute(thread)
          }
        }
      }finally {
        threadPool.shutdown()
        threadPool.awaitTermination(sendNum.toInt*threadNum.toInt*1000,TimeUnit.MILLISECONDS)
        client.destroy()
      }
    }else{
      println("初始化失败，请检测远程storm是否启动")
    }
  }
}
