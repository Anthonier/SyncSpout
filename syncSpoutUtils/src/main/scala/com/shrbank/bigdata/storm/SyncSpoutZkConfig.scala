package com.shrbank.bigdata.storm

import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ShaoJie.Wu on 2017/6/9 11:18.
  * 功能：
  *
  * 备注：
  */
object SyncSpoutZkConfig{
  val ZK_ROOT_PATH = "/SyncSpout"
  val SERVER_TYPE = "Server"
  val CLIENT_TYPE = "Client"
  // 某节点可用时回调函数类型
  type ConnectFunctionType = (Array[(String,Int)])=>Unit
  type ReConnectFunctionType = ()=>Unit
  val retryPolicy = new ExponentialBackoffRetry(1000,3)
}
class SyncSpoutZkConfig {
  private val log = LoggerFactory.getLogger(this.getClass)
  private var zkClient: CuratorFramework  = _
  private var topologyName: String = _
  private var serverPath: String = _
  private var reConnectF : SyncSpoutZkConfig.ReConnectFunctionType = _
  private var zkListener:PathChildrenCacheListener = _
  private var pathChildrenCache:PathChildrenCache = _
  def getZkClient = zkClient

  def this(hosts: String) {
    this()
    zkClient = CuratorFrameworkFactory.newClient(hosts, SyncSpoutZkConfig.retryPolicy)
    zkClient.start()
    zkListener = new ZKNodeListener(this.serverChange)
  }

  def this(hosts: String, topologyName: String) {
    this(hosts)
    this.topologyName = topologyName
    serverPath = s"${SyncSpoutZkConfig.ZK_ROOT_PATH}/${this.topologyName}"
    log.debug(s"NewSyncSpoutZkConfig init ,hosts = $hosts,serverPath = $serverPath")
    createRootPath()
    pathChildrenCache = new PathChildrenCache(zkClient,serverPath,true)
    createServerPath()
  }

  private def getServerPortPath(host: String, port: Int) = s"$serverPath/$host-$port"

  private def createRootPath() = {
    try{
      zkClient.create().withMode(CreateMode.PERSISTENT).forPath(SyncSpoutZkConfig.ZK_ROOT_PATH,"".getBytes)
    }catch {
      case ex:NodeExistsException=>
    }
  }

  private def deleteRootPath() = zkClient.delete().forPath(SyncSpoutZkConfig.ZK_ROOT_PATH)

  private def createServerPath() = {
    try{
      zkClient.create().withMode(CreateMode.PERSISTENT).forPath(serverPath, "".getBytes)
    }catch {
      case ex:NodeExistsException=>
    }
  }
  def deleteServerPath() = zkClient.delete().forPath(serverPath)

  /**
    * 将SyncSpout使用的ip，客户端临时插入到zk中，供client使用
    *
    * @param host SyncSpout当前所在的IP
    * @param port SyncSpout当前占用的端口
    * @return 插入成功返回true；否则返回false
    */
  def registerServerPort(host: String, port: Int) = registerPort(host, port, SyncSpoutZkConfig.SERVER_TYPE)

  /**
    * 将SyncSpout某个server退出后，清空zk配置
    *
    * @param host SyncSpout当前所在的IP
    * @param port SyncSpout当前占用的端口
    * @return 删除成功返回true；否则返回false
    */
  def unRegisterServerPort(host: String, port: Int) = unRegisterPort(host, port)

  /**
    * 查询当前注册的SyncSpout端口
    *
    * @return 返回知道指定Spout注册的IP列表，形式是IP:PORT
    */
  def getServerPort: Array[(String,Int)] = {
    val serverList = new ArrayBuffer[(String,Int)]()
    val hosts = zkClient.getChildren.forPath(serverPath).iterator()
    // 在查询到children列表之后，其中的节点可能会被删除掉，所以需要判断是否存在，以增加其健壮性
    while(hosts.hasNext) {
      val child = hosts.next()
      //val buffer = zkClient.getData[Array[Byte]](s"$serverPath/$child",true)
      val buffer = zkClient.getData.forPath(s"$serverPath/$child")
      if( buffer != null ){
        val serverType = new String( buffer )
        if (serverType.split("-")(0) == SyncSpoutZkConfig.SERVER_TYPE){
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
  def registerClientPort(host: String, port: Int) = registerPort(host, port, SyncSpoutZkConfig.CLIENT_TYPE)

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
      zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path,registerType.getBytes)
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
    zkClient.delete().forPath(path)
  }

  def close() = zkClient.close()

  /**
    * 监控此次server的变化
    *
    * @param watcher server变化时执行的函数
    */
  def watchServer(watcher: SyncSpoutZkConfig.ReConnectFunctionType): Unit = {
    if( null == this.reConnectF ){
      pathChildrenCache.getListenable.addListener(zkListener)
      pathChildrenCache.start()
      this.reConnectF = watcher
    }
  }

  /**
    * 取消对server的监控
    */
  def unWatchServer(): Unit = {
    this.reConnectF = null
    pathChildrenCache.getListenable.clear()
    pathChildrenCache.close()
  }
  def serverChange():Unit = {
    log.info(s"serverChange,reConnectF=$reConnectF")
    if( reConnectF != null )
      reConnectF()
  }
}
class ZKNodeListener(nodeChange:()=>Unit) extends PathChildrenCacheListener{
  private val log = LoggerFactory.getLogger(classOf[ZKNodeListener])

  override def childEvent(curatorFramework: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
//    event.getType match {
//      case PathChildrenCacheEvent.Type.CHILD_ADDED =>
//      case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
//      case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
//    }
    nodeChange()
    log.info(s"CuratorFramework receive an event = ${event.getType},path = ${event.getData.getPath},path type = ${new String(event.getData.getData,"UTF-8")}")
  }
}