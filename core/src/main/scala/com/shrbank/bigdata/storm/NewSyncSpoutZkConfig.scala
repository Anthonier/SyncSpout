package com.shrbank.bigdata.storm

import java.util

import com.shrbank.bigdata.zkclient.{ZKClient, ZKClientBuilder}
import com.shrbank.bigdata.zkclient.exception.ZKNodeExistsException
import com.shrbank.bigdata.zkclient.listener.{ZKChildDataListener, ZKNodeListener}
import com.shrbank.bigdata.zkclient.serializer.BytesSerializer
import org.apache.zookeeper._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
/**
  * Created by wushaojie on 2016/9/18.
  * 负责向zookeeper读写指定的配置
  * 此处设计的不太好，后期需要重构。2016年9月26日
  * 耦合性太强了啊。2016年9月29日
  */

object NewSyncSpoutZkConfig{
  val ZK_ROOT_PATH = "/SyncSpout"
  val SERVER_TYPE = "Server"
  val CLIENT_TYPE = "Client"
  // 某节点可用时回调函数类型
  type ConnectFunctionType = (Array[(String,Int)])=>Unit
  type ReConnectFunctionType = ()=>Unit
}
class NewSyncSpoutZkConfig {
  private val log = LoggerFactory.getLogger(this.getClass)
  private var zkClient: ZKClient = _
  private var topologyName: String = _
  private var serverPath: String = _
  private var reConnectF : NewSyncSpoutZkConfig.ReConnectFunctionType = _
  def getZkClient = zkClient

  def this(hosts: String) {
    this()
    zkClient = ZKClientBuilder.newZKClient(hosts).serializer(new BytesSerializer()).build()
  }

  def this(hosts: String, topologyName: String) {
    this(hosts)
    this.topologyName = topologyName
    serverPath = s"${NewSyncSpoutZkConfig.ZK_ROOT_PATH}/${this.topologyName}"
    log.debug(s"NewSyncSpoutZkConfig init ,hosts = $hosts,serverPath = $serverPath")
    createRootPath()
    createServerPath()
  }

  private def getServerPortPath(host: String, port: Int) = s"$serverPath/$host-$port"

  private def createRootPath() = {
    try{
      zkClient.create(NewSyncSpoutZkConfig.ZK_ROOT_PATH, "".getBytes, CreateMode.PERSISTENT)
    }catch {
      case ex:ZKNodeExistsException=>
    }
  }

  private def deleteRootPath() = zkClient.delete(NewSyncSpoutZkConfig.ZK_ROOT_PATH)

  private def createServerPath() = {
    try{
      zkClient.create(serverPath, "".getBytes, CreateMode.PERSISTENT)
    }catch {
      case ex:ZKNodeExistsException=>
    }
  }
  def deleteServerPath() = zkClient.delete(serverPath)

  /**
    * 将SyncSpout使用的ip，客户端临时插入到zk中，供client使用
    *
    * @param host SyncSpout当前所在的IP
    * @param port SyncSpout当前占用的端口
    * @return 插入成功返回true；否则返回false
    */
  def registerServerPort(host: String, port: Int) = registerPort(host, port, NewSyncSpoutZkConfig.SERVER_TYPE)

  /**
    * 将SyncSpout某个server退出后，清空zk配置
    *
    * @param host SyncSpout当前所在的IP
    * @param port SyncSpout当前占用的端口
    * @return 删除成功返回true；否则返回false
    */
  def unRegisterServerPort(host: String, port: Int) = unRegisterPort(host, port)

//  /**
//    * 更新server节点的启动时间，以通知client
//    *
//    * @param host SyncSpout当前所在的IP
//    * @param port SyncSpout当前占用的端口
//    */
//  def upServer(host: String, port: Int): Unit = {
//    // val path = getServerPortPath(host,port)
//    val path = serverPath
//    val currentTime = System.currentTimeMillis()
//    log.debug(s"upServerPort,$host,$port,$currentTime")
//    if ( zkClient.exists( path ) )
//      zkClient.setData(path, s"${NewSyncSpoutZkConfig.SERVER_TYPE}-$currentTime".getBytes)
//  }

  /**
    * 查询当前注册的SyncSpout端口
    *
    * @return 返回知道指定Spout注册的IP列表，形式是IP:PORT
    */
  def getServerPort: Array[(String,Int)] = {
    val serverList = new ArrayBuffer[(String,Int)]()
    val hosts = zkClient.getChildren(serverPath).iterator()
    // 在查询到children列表之后，其中的节点可能会被删除掉，所以需要判断是否存在，以增加其健壮性
    while(hosts.hasNext) {
      val child = hosts.next()
      val buffer = zkClient.getData[Array[Byte]](s"$serverPath/$child",true)
      if( buffer != null ){
        val serverType = new String( buffer )
        if (serverType.split("-")(0) == NewSyncSpoutZkConfig.SERVER_TYPE){
          val Array(host,port) = child.split("-")
          serverList.append((host,port.toInt))
        }
      }

    }
    log.debug(s"getServerPort return value,${serverList.mkString(",")}")
    serverList.toArray
  }

  /**
    * 将SyncSpoutClient使用的ip，客户端临时插入到zk中，供client使用
    *
    * @param host SyncSpoutClient当前所在的IP
    * @param port SyncSpoutClient当前占用的端口
    * @return 插入成功返回true；否则返回false
    */
  def registerClientPort(host: String, port: Int) = registerPort(host, port, NewSyncSpoutZkConfig.CLIENT_TYPE)

  /**
    * 将SyncSpoutClient某个server退出后，清空zk配置
    *
    * @param host SyncSpoutClient当前所在的IP
    * @param port SyncSpoutClient当前占用的端口
    * @return 删除成功返回true；否则返回false
    */
  def unRegisterClientPort(host: String, port: Int) = unRegisterPort(host, port)

  /**
    * 向zk注册临时节点，区分server和client
    *
    * @param host         当前所在的IP
    * @param port         当前占用的端口
    * @param registerType 注册的类型，分为SERVER_TYPE和CLIENT_TYPE
    * @return 插入成功返回true；否则返回false
    */
  private def registerPort(host: String, port: Int, registerType: String) = {
    val path = getServerPortPath(host, port)
    var isSuccess = false
    try{
      zkClient.create(path, registerType.getBytes, CreateMode.EPHEMERAL)
      isSuccess = true
    }catch{
      case ex:Exception =>
    }
    isSuccess
  }

  /**
    * 将SyncSpout某个server退出后，清空zk配置
    *
    * @param host 当前所在的IP
    * @param port 当前占用的端口
    * @return 删除成功返回true；否则返回false
    */
  private def unRegisterPort(host: String, port: Int) = {
    val path = getServerPortPath(host, port)
    zkClient.delete(path)
  }

  def close() = zkClient.close()

  /**
    * 监控此次server的变化
    *
    * @param watcher server变化时执行的函数
    */
  def watchServer(watcher: NewSyncSpoutZkConfig.ReConnectFunctionType): Unit = {
    if( null == this.reConnectF ){
      zkClient.listenChildDataChanges(serverPath,new MyZKChildDataListener(this.serverChange))
      zkClient.listenNodeChanges(serverPath,new MyZKNodeListener(this.serverChange))
      this.reConnectF = watcher
    }
  }

  /**
    * 取消对server的监控
    */
  def unWatchServer(): Unit = {
    this.reConnectF = null
    zkClient.unlistenAll()
  }
  def serverChange():Unit = {
    log.info(s"serverChange,reConnectF=$reConnectF")
    if( reConnectF != null )
      reConnectF()
  }
}
class MyZKNodeListener(nodeChange:()=>Unit) extends ZKNodeListener{
  private val log = LoggerFactory.getLogger(this.getClass)
  override def handleDataChanged(path: String, nodeData: scala.Any): Unit = {
    val data = if(null != nodeData) new String ( nodeData.asInstanceOf[Array[Byte]] ) else ""
    log.info(s"handleDataChanged path=$path,data=$data")
    nodeChange()
  }

  override def handleDataDeleted(path: String): Unit = {
//    log.info(s"handleDataDeleted path=$path")
  }

  override def handleDataCreated(path: String, nodeData: scala.Any): Unit = {
//    val data = if(null != data) new String ( nodeData.asInstanceOf[Array[Byte]] ) else ""
//    log.info(s"handleDataCreated path=$path,data=$data")
  }

  override def handleSessionExpired(path: String): Unit = {
//    log.info(s"handleSessionExpired path=$path")
  }
}
class MyZKChildDataListener(nodeChange:()=>Unit) extends ZKChildDataListener {
  private val log = LoggerFactory.getLogger(this.getClass)
  override def handleChildCountChanged(path: String, childList: util.List[String]): Unit = {
    // 子节点数量变化时（增加或减少）都需要重连服务器
    if(log.isDebugEnabled){
      log.info(s"handleChildCountChanged,path=$path")
      val size = childList.size()
      for(i<-0 until size){
        val Array(ip,port) = childList.get(i).split("-")
        log.info(s"child[$i]=${childList.get(i)},ip=$ip,port=$port")
      }
    }
    nodeChange()
  }

  override def handleChildDataChanged(path: String, nodeData: scala.Any): Unit = {
//    val data = if(null != nodeData) new String ( nodeData.asInstanceOf[Array[Byte]] ) else ""
//    log.info(s"handleChildDataChanged path=$path,data=$data")
  }

  override def handleSessionExpired(path: String, nodeData: scala.Any): Unit = {
//    val data = if(null != data) new String ( nodeData.asInstanceOf[Array[Byte]] ) else ""
//    log.info(s"handleSessionExpired s=$path,data=$data")
  }

}