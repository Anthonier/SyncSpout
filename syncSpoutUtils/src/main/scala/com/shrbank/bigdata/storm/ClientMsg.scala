package com.shrbank.bigdata.storm

/**
  * Created by wushaojie on 2016/9/9.
  * 向服务器发送的消息，其中msg为任意类型的AnyRef
  * change
  * 增加msgTime字段，给消息一个时间戳，以度量性能
  * 2017.04.14 修改msgSendTime、msg字段命名
  */
case class ClientMsg(message:AnyRef, sendTime:Long = System.currentTimeMillis)
