package com.lai.canalsyn.client.elasticsearch.storage.impl;


import com.lai.canalsyn.client.elasticsearch.enums.OperateType;
import com.lai.canalsyn.client.elasticsearch.module.ESIncStorageWrapper;
import com.lai.canalsyn.client.elasticsearch.module.EsFullStorageWrapper;
import com.lai.canalsyn.client.elasticsearch.storage.EsFullStorageSupport;
import com.lai.canalsyn.client.elasticsearch.storage.EsIncStorageSupport;
import com.lai.canalsyn.client.elasticsearch.util.EsOperateUtil;
import com.lai.canalsyn.mapper.DynasticTableMapper;
import com.lai.canalsyn.message.Dml;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.util.*;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/21 14:08
 * @ Description :  增量和全量实现模板
 * 约定
 * 主键都是id
 * 主键只有一个
 */
@Slf4j
public abstract class AbstractESStorageSupport implements EsFullStorageSupport, EsIncStorageSupport<Dml> {
    @Autowired
    DynasticTableMapper dynasticTableMapper;

    @Autowired
    RestHighLevelClient client;



    @Override
    public boolean isSupportIncStorageConvert(Dml dml) {
        return dml.getTable().equals(getTableName());
    }

    /**
     * 全量同步条件
     * 1.支持的表在系统中存在
     * 2.有相应的索引结构
     * 3.Es引擎还没有改表 或者 有该表但是没有对应的数据库id
     * @param dbName
     * @return
     */
    @Override
    public boolean isSupportFullStorageConvert(String dbName) {
        return dynasticTableMapper.isExist(dbName, getTableName()) != null && EsOperateUtil.IndexAPI.createIndex(client, getIndecis(dbName), getAliases(dbName), getScheme());
    }

    /**
     * 全量转化处理
     *
     * @param dbName
     * @return
     */
    @Override
    public EsFullStorageWrapper doFullStorageConvert(String dbName) {
        List<Map<String, Object>> datamaps = dynasticTableMapper.getInfos(dbName, getTableName());
        fullPreProcess(datamaps);
        EsFullStorageWrapper build = EsFullStorageWrapper.builder()
                .indicsName(getIndecis(dbName))
                .pkName(getPKName())
                .dataMappings(datamaps)
                .aliases(getAliases(dbName))
                .scheme(getScheme()).build();
        log.info("正在全量同步的表信息:      数据库为：[{}] ;数据表为： [{}] ,数据数量为：[{}].......", dbName, getTableName(), datamaps.size());
        return build;
    }

    /**
     * 增量转化处理
     *
     * @param dml
     * @return
     */
    @Override
    public List<ESIncStorageWrapper> doIncStorageConvert(Dml dml) {
        ArrayList<ESIncStorageWrapper> esWrapperList = new ArrayList<>();
        List<Map<String, Object>> datas = dml.getData();
        incPreProcess(datas);
        if (!datas.isEmpty()) {
            for (Map<String, Object> data : datas) {
                ESIncStorageWrapper build = ESIncStorageWrapper.builder()
                        .indicsName(getIndecis(dml.getDatabase()))
                        .documentId(data.get(dml.getPkNames().get(0)).toString())
                        .type(OperateType.chooseOperateType(dml))
                        .dataMapping(parseMapping(data))
                        .scheme(getScheme())
                        .aliases(getAliases(dml.getDatabase()))
                        .build();
                esWrapperList.add(build);
            }
        }

        return esWrapperList;
    }


    /**
     * 获取保存表名称
     *
     * @return
     */
    public abstract String getTableName();

    /**
     * 解析映射
     * 默认mysql的字段和索引字段互相对应
     *
     * @return
     */
    public Map parseMapping(Map map) {
        return map;
    }

    /**
     * 获取索引结构
     *
     * @return
     */
    protected abstract Map getScheme();

    /**
     * 获取索引别名
     *
     * @return
     */
    private Set getAliases(String dnName) {
        HashSet<String> aliasSet = new HashSet<>();
        aliasSet.add(getIndecis(dnName) + "_alias");
        return aliasSet;
    }

    /**
     * 默认索引规则
     *
     * @return
     */
    public String getIndecis(String dbName) {
        return dbName + "_" + getTableName();
    }

    /**
     * 默认主键名称
     *
     * @return
     */
    public String getPKName() {
        return "id";
    }


    /**
     * 增量存储前置处理
     */
    private void incPreProcess(List<Map<String, Object>> datamaps) {
        typeHandle(datamaps);

    }

    /**
     * 增量存储后置处理
     */
    private void incPostProcess() {

    }

    /**
     * 全量存储前置处理
     */
    private final  void fullPreProcess(List<Map<String, Object>> datamaps) {
       typeHandle(datamaps);

    }
    private  void typeHandle(List<Map<String, Object>> datamaps){
        for (Map<String, Object> datamap : datamaps) {
            Set<Map.Entry<String, Object>> entries = datamap.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                if(entry.getValue() instanceof Timestamp){
                    Timestamp timestamp =(Timestamp) entry.getValue();
                    datamap.put(entry.getKey(),new Date(timestamp.getTime()));
                }

            }
        }
    }


    /**
     * 全量存储后置处理
     */
    private void fullPostProcess() {

    }


}
