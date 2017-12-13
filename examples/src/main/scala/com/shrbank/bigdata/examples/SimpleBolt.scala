package com.shrbank.bigdata.examples

import com.shrbank.bigdata.storm.{SyncBoltFields, SyncBoltValues, Utils}
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.LoggerFactory

/**
  * Created by wushaojie on 2016/9/9.
  * 该bolt接收SyncSpout的消息，进行简单计算并返回给SendBolt
  */
class SimpleBolt extends BaseBasicBolt{
  private val log = LoggerFactory.getLogger(this.getClass)
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val msg = "这是处理后的消息，可以添加自己的逻辑-" + tuple.getValueByField(Utils.SendBoltOutputFieldName).
      asInstanceOf[String] // 此处的类型必须与ask函数中ClientMsg的构造参数类型一致
    log.info(s"SimpleBolt:$msg")
    // 需要通过SendBolt发送个客户端的消息，必须定义在SendBolt.OutputFieldName字段中
    basicOutputCollector.emit(SyncBoltValues(tuple,msg,Integer.valueOf(msg.length)))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    // 定义SendBolt.OutputFieldName字段，SendBolt将SendBolt.OutputFieldName字段中的数据返回给客户端
    outputFieldsDeclarer.declare(SyncBoltFields(Utils.SendBoltOutputFieldName,"MessageLength"))
  }
}