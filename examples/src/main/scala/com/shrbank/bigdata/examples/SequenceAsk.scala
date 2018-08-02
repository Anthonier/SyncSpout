package com.shrbank.bigdata.examples

import com.shrbank.bigdata.storm.{ClientMsg, SyncSpoutClient}

/**
  * Created by ShaoJie.Wu on 2017/4/11 12:53.
  * 功能：
  *
  * 备注：
  */
object SequenceAsk {
  val client = new SyncSpoutClient("SyncSpoutTop10")
  def main(args: Array[String]): Unit = {
    val Array(totalNum) = args
    var avg = 0L
    var sum = 0L
    if(client.init()){
      for(i<-1 to totalNum.toInt ){
        val start = System.currentTimeMillis()
        client.ask(ClientMsg("测试"),1000)
        val end = System.currentTimeMillis()
        sum += (end - start)
        avg = sum / i
        println(s"当前消息$i-$totalNum，耗时 $avg 毫秒")

      }
      println(s"共计 ${totalNum} 条消息，平均耗时 $avg 毫秒")
    }
    client.destroy()
  }
}
