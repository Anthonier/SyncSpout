package com.shrbank.bigdata.storm

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

/**
  * Created by wushaojie on 2016/10/9.
  * 暂时没用
  */
class ClientExecutionContext extends ExecutionContext {
  val threadPool = Executors.newFixedThreadPool(1000)
  override def execute(runnable: Runnable): Unit = {
    threadPool.submit(runnable)
  }

  override def reportFailure(t: Throwable): Unit = {

  }
}
