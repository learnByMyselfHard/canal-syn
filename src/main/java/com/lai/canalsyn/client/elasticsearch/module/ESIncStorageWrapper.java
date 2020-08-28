package com.lai.canalsyn.client.elasticsearch.module;


import com.lai.canalsyn.client.elasticsearch.enums.OperateType;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Set;

/**
 * Es增量存储包装对象
 */
@Data
@Builder
public class ESIncStorageWrapper {


    /**
     * Es的数据映射存储
     */
    private Map<String, Object> dataMapping;
    /**
     * 文档请求类型
     * 参考{@link OperateType}
     *
     */
    private OperateType type;


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