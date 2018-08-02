package com.shrbank.bigdata.storm
import org.apache.storm.tuple.{Fields, Tuple, Values}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by wushaojie on 2016/9/9.
  * 用来代替new Values,new Fields创建values，fields对象
  * 这是因为最后的SendBolt需要SyncSpout.TupleFieldName，SyncSpoutClient.TupleFieldName两个字段
  */
object SyncBoltValues {
  def apply(tuple:Tuple,values: AnyRef * ) :Values = {
    val server = tuple.getValueByField(Utils.SpoutTupleFieldName)
    val client = tuple.getValueByField(Utils.ClientTupleFieldName)
    val msgSendTime = tuple.getValueByField(Utils.MsgSendTimeFieldName)
    val syncBoltValues = new ArrayBuffer[AnyRef]()
    syncBoltValues.appendAll(values)
    syncBoltValues.append(server,client,msgSendTime)
    new Values(syncBoltValues :_*)
  }
}
object SyncBoltFields {
  def apply(fields: String* ) :Fields= {
    val syncSpoutFields = new ArrayBuffer[String]()
    syncSpoutFields.appendAll(fields)
    syncSpoutFields.append(Utils.SpoutTupleFieldName,Utils.ClientTupleFieldName,Utils.MsgSendTimeFieldName)
    new Fields( syncSpoutFields :_* )
  }
}
