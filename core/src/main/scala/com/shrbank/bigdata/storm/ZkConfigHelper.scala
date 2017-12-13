package com.shrbank.bigdata.storm

import org.slf4j.LoggerFactory

/**
  * Created by wushaojie on 2016/9/20.
  * 查看当前SyncSpout在Zk中的配置情况
  */
object ZkConfigHelper {
  private val log = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    if ( args.length < 3 ){
      log.error("用法：ZkConfigHelper zkServerList list|rm [topologyName|all]")
      println("用法：ZkConfigHelper zkServerList list|rm [topologyName|all]")
      return
    }
    val Array( zkServer,cmd,topologyName ) = args
    log.info(s"查找[$zkServer]下[$topologyName]的配置情况")
    println(s"查找[$zkServer]下[$topologyName]的配置情况")
    var zkConfig:SyncSpoutZkConfig = null
    if(topologyName.toLowerCase=="all"){
      zkConfig = new SyncSpoutZkConfig(zkServer)
      val children = zkConfig.getZkClient.getChildren.forPath(SyncSpoutZkConfig.ZK_ROOT_PATH)
      println(s"$cmd 运行中的Spout列表")
      for(i<-0 until children.size()){
        println(s"$i,${children.get(i)}")
        val childrenPath = s"${SyncSpoutZkConfig.ZK_ROOT_PATH}/${children.get(i)}"
        val server = zkConfig.getZkClient.getChildren.forPath(childrenPath)
        if(cmd.toLowerCase=="list"){
          println(s"${children.get(i)} 分布详情")
          for(j<-0 until server.size()){
            println(s"  $j,${server.get(j)},type = ${new String(zkConfig.getZkClient.getData.forPath(s"$childrenPath/${server.get(j)}"))}")
          }
        }else{
          val children = zkConfig.getZkClient.getChildren.forPath(childrenPath)
          if(children.isEmpty){
            zkConfig.getZkClient.delete.forPath(childrenPath)
            println(s"$childrenPath 已删除")
          }else{
            for(i<-0 until children.size()){
              zkConfig.getZkClient.delete.forPath(s"$childrenPath/${children.get(i)}")
              println(s"$childrenPath/${children.get(i)} 已删除")
            }
          }
        }
      }
    }else{
      zkConfig = new SyncSpoutZkConfig(zkServer,topologyName)
      println(s"$topologyName 分布详情")
      if(cmd.toLowerCase=="list"){
        zkConfig.getServerPort.foreach(println)
      }else{
        if(zkConfig.getServerPort.isEmpty){
          zkConfig.deleteServerPath()
          println(s"$topologyName 已删除")
        }
      }
    }
     zkConfig.close()
  }
}
