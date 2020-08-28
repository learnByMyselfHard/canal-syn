package com.lai.canalsyn.client.elasticsearch.storage;


import com.lai.canalsyn.client.elasticsearch.module.ESIncStorageWrapper;

import java.util.List;

/**
 * @ Author : lai
 * @ Date   : created in  2020/5/13 11:36
 * @ Description :  增量同步
 */
public interface EsIncStorageSupport<T>{
    //增量
    List<ESIncStorageWrapper> doIncStorageConvert(T dml);
    default boolean isSupportIncStorageConvert(T dml) {
        return true;
    }
}
