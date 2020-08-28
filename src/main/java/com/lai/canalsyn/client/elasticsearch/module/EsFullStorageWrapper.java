package com.lai.canalsyn.client.elasticsearch.module;  //全量同步的Es包装

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Es全量存储包装对象
 */
@Data
@Builder
public  class EsFullStorageWrapper{

    /**
     * Es的数据映射存储
     */
    private List<Map<String, Object>> dataMappings;

    /**
     * 主键Id
     */
    private String pkName;


    /**
     * 索引名称
     */
    protected String indicsName;

    /**
     * 文档Id  对应mysql的主键Id
     */
    protected String documentId;

    /**
     * Es的表结构定义
     */
    protected Map<String, Object> scheme;



    /**
     * Es的别名
     */
    protected Set<String> aliases;
}