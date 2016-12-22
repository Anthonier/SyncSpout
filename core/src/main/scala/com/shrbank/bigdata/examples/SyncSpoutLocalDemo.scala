package com.shrbank.bigdata.examples

import com.shrbank.bigdata.storm.{SendBolt, SyncSpout}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.{Config, LocalCluster}

/**
  * Created by wushaojie on 2016/9/8.
  * SyncSpout本地模式
  */
object SyncSpoutLocalDemo {
  def main(args: Array[String]) {
    val builder = new TopologyBuilder()
    // ActorSpout用于接收消息
    builder.setSpout("syncSpout",SyncSpout(),2)
    // SimpleBolt用于处理消息
    builder.setBolt("simpleBolt",new SimpleBolt(),2).setNumTasks(4).shuffleGrouping("syncSpout")
    // SendBolt用于返回消息
    builder.setBolt("sendBolt",new SendBolt(),2).shuffleGrouping("simpleBolt")
    val cluster = new LocalCluster()
    val topName = "SyncSpoutTop" //SyncSpoutTop,recommend_system_purchase
    val conf = new Config()
    conf.setNumWorkers(2)
    cluster.submitTopology(topName,conf,builder.createTopology())
    println( "SyncSpout 启动成功!" )
  }
}