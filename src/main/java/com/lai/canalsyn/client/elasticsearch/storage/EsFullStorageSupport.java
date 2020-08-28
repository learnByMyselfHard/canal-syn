package com.lai.canalsyn.client.elasticsearch.storage;


import com.lai.canalsyn.client.elasticsearch.module.EsFullStorageWrapper;

/**
 * @ Author : lai
 * @ Date   : created in  2020/5/13 11:35
 * @ Description :  全量同步
 */
public interface EsFullStorageSupport {

    //全量
    EsFullStorageWrapper doFullStorageConvert(String dbName);
    default boolean isSupportFullStorageConvert(String dbName) {
        return true;
    }

}
