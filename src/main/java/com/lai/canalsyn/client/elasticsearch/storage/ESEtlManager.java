package com.lai.canalsyn.client.elasticsearch.storage;


import com.lai.canalsyn.client.elasticsearch.module.EsFullStorageWrapper;
import com.lai.canalsyn.client.elasticsearch.util.EsOperateUtil;
import com.lai.canalsyn.mapper.DataBaseInfoMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/23 12:27
 * @ Description : 全量同步管理
 */
@Slf4j
@Service
public class ESEtlManager implements InitializingBean {
    @Autowired(required = false)
    List<EsFullStorageSupport> esStorageSupports;
    @Autowired
    RestHighLevelClient client;
    @Autowired
    DataBaseInfoMapper dataBaseInfoMapper;

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    public void doFullStorage() throws IOException {
        long start = System.currentTimeMillis();
        log.info("开始全量同步！！");
        AtomicLong total = new AtomicLong();
        List<String> allNeedImportDatabase = getAllNeedImportDatabase();
        for (String dbName : allNeedImportDatabase) {
            //todo  如果全量同步很慢的话这里可以加入多线程
            for (EsFullStorageSupport esFullStorageSupport : esStorageSupports) {
                if (esFullStorageSupport.isSupportFullStorageConvert(dbName)) {
                    EsFullStorageWrapper esFullStorageWrapper = esFullStorageSupport.doFullStorageConvert(dbName);
                    total.addAndGet(esFullStorageWrapper.getDataMappings().size());
                    //同步Es
                    doBatch(esFullStorageWrapper);
                }
            }
        }
        long end = System.currentTimeMillis();
        ConcurrentHashMap.KeySetView<String, Integer> cacheIndicsKeys = EsOperateUtil.getIndicsExistCache().keySet();
        StringBuilder cacheIndicsKeysString = new StringBuilder();
        for (String cacheIndicsKey : cacheIndicsKeys) {
            cacheIndicsKeysString.append(cacheIndicsKey + "\n");
        }

        log.info("\n"+"全量同步完成！！, 花费时间为[{}], 创建的索引列表为["+"\n"+"{}] , 同步的数量记录数为[{}]：", end - start, cacheIndicsKeysString, total.get());


    }

    public void doBatch(EsFullStorageWrapper esFullStorageWrapper) {
        ArrayList<DocWriteRequest> bulkQuest = new ArrayList<>();
        List<Map<String, Object>> dataMappings = esFullStorageWrapper.getDataMappings();
        for (Map<String, Object> dataMapping : dataMappings) {
            bulkQuest.add(EsOperateUtil.DocumentAPI.insert(esFullStorageWrapper.getIndicsName(), String.valueOf(dataMapping.get(esFullStorageWrapper.getPkName())), dataMapping));
        }
        EsOperateUtil.bulk(client, bulkQuest);
    }


    public List<String> getAllNeedImportDatabase() {
        List<String> allDataBases = dataBaseInfoMapper.getAllDataBases();
        Set<String> filterDbNameMap = needFilterDbNameMap();
        Iterator<String> iterator = allDataBases.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (filterDbNameMap.contains(next)) {
                iterator.remove();
            }
        }
        return allDataBases;
    }


    //后续移到配置文件，用于过滤不需要同步的库
    public Set<String> needFilterDbNameMap() {
        Set<String> filterSet = new HashSet<>();
        filterSet.add("information_schema");
        filterSet.add("performance_schema");
        return filterSet;
    }


}
