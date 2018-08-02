package com.shrbank.bigdata.storm

import akka.actor.{Actor, ActorLogging}

/**
  * Created by ShaoJie.Wu on 2017/4/11 11:05.
  * 功能：
  *
  * 备注：
  */
trait IMessageSender{
  def send(input:SpoutInputMsg):Unit
}
/**
  * 消息转发的actor
  * 该actor用来接收client发送的消息，并调用IMessageSender.send将该消息发送出去
  */
private[storm] class MessageActor( messageSender:IMessageSender ) extends Actor with ActorLogging{

  override def receive: Receive =  {
    case ClientMsg(msg,msgSendTime) =>
      messageSender.send(SpoutInputMsg(this.sender().path,msg,msgSendTime.toString))
      log.debug(s"收到客户端消息$msg,耗时${System.currentTimeMillis()-msgSendTime}")
    case unKnownTypeMsg =>
      this.sender() ! ServerMsg(s"不支持的消息类型($unKnownTypeMsg)，请用ClientMsg进行封装")
      log.error(s"非法消息 $unKnownTypeMsg")
  }
}
