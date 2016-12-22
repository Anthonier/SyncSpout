package com.shrbank.bigdata.storm.queue;

/**
 * Created by wushaojie on 2016/10/18.
 * ISpoutMessageQueue接口
 */
public interface ISpoutMessageQueue<T> {
    boolean add(T node);
    T poll();
    boolean isEmpty();
}
