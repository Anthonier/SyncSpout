package com.shrbank.bigdata.examples

import com.shrbank.bigdata.storm.{SendBolt, SyncSpout}
import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder


/**
  * Created by wushaojie on 2016/9/8.
  * SyncSpout集群模式
  */
object SyncSpoutClusterDemo {
  def main(args: Array[String]) {
    val builder = new TopologyBuilder()
    // ActorSpout用于接收消息
    builder.setSpout( "syncSpout",SyncSpout(),2)
    // SimpleBolt用于处理消息
    builder.setBolt("simpleBolt",new SimpleBolt(),2).setNumTasks(4).shuffleGrouping("syncSpout")
    // SendBolt用于返回消息
    builder.setBolt("sendBolt",new SendBolt(),2).shuffleGrouping("simpleBolt")
    val topName = "SyncSpoutTop"
    val conf = new Config()
    // 使用两个worker进程
    conf.setNumWorkers(2)
    StormSubmitter.submitTopology(topName,conf,builder.createTopology())
    println( "SyncSpout 启动成功!" )
  }
}
