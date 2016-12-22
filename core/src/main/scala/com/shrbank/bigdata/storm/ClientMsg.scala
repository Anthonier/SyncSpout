package com.shrbank.bigdata.storm

/**
  * Created by wushaojie on 2016/9/9.
  * 向服务器发送的消息，其中msg为任意类型的AnyRef
  * change
  * 增加msgTime字段，给消息一个时间戳，以度量性能
  */
case class ClientMsg(msg:AnyRef, msgSendTime:Long = System.currentTimeMillis)
