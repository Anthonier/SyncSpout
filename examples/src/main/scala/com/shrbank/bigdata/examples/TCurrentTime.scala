package com.shrbank.bigdata.examples

/**
  * Created by ShaoJie.Wu on 2017/4/14 15:19.
  * 功能：
  *
  * 备注：
  */
object TCurrentTime {
  def main(args: Array[String]): Unit = {
    val num = 10000
    var end:Long = 0
    val start = System.currentTimeMillis()
    for(i<-0 until num) {
      end = System.currentTimeMillis()
    }
    println(s"currentTimeMillis耗时${end-start}毫秒")
  }
}
