package com.lai.canalsyn.client.elasticsearch.storage;


import com.alibaba.fastjson.JSON;
import com.lai.canalsyn.client.elasticsearch.enums.OperateType;
import com.lai.canalsyn.client.elasticsearch.module.ESIncStorageWrapper;
import com.lai.canalsyn.client.elasticsearch.util.EsOperateUtil;
import com.lai.canalsyn.message.Dml;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/21 11:49
 * @ Description : 增量同步管理
 */
@Slf4j
@Service
public class ESIncStorageManager implements InitializingBean {
    @Autowired(required = false)
    List<EsIncStorageSupport> esStorageSupports;
    @Autowired
    RestHighLevelClient client;


    @Override
    public void afterPropertiesSet() throws Exception {
        log.debug("EsStorageSupport InitializingBean");
        if (CollectionUtils.isEmpty(esStorageSupports)) {
            log.error("not config sEsStorageSupport");
            //如果配置storage则直接异常退出
            System.exit(1);
        }
        log.info("ES存储支持器:{} 总共:[{}] 个支持器", JSON.toJSONString(getStorageClassInfo(esStorageSupports)), esStorageSupports.size());

    }

    public void doIncStorage(List<Dml> dmlList) throws IOException {
        //转换器
        ArrayList<ESIncStorageWrapper> esWrapperArrayList = new ArrayList<>();
        for (Dml dml : dmlList) {
            for (EsIncStorageSupport esIncStorageSupport : esStorageSupports) {
                if (esIncStorageSupport.isSupportIncStorageConvert(dml)) {
                    List<ESIncStorageWrapper> esWrappers = esIncStorageSupport.doIncStorageConvert(dml);
                    esWrapperArrayList.addAll(esWrappers);
                }
                ;
            }
        }
        //创建索引
        for (ESIncStorageWrapper esWrapper : esWrapperArrayList) {
            EsOperateUtil.IndexAPI.createIndex(client, esWrapper.getIndicsName(), esWrapper.getAliases(), esWrapper.getScheme());
        }
        //文档批处理
        try {
            log.debug("批处理数据");
            log.debug(esWrapperArrayList.toString());
            doBatch(esWrapperArrayList);
        } catch (
                Exception e) {
            log.info(e.getMessage());
        }

    }


    private List<String> getStorageClassInfo(List<EsIncStorageSupport> storages) {
        if (!CollectionUtils.isEmpty(storages)) {
            return storages.stream().map(storage -> storage.getClass().getName()).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }


    //批量保存
    private void doBatch(List<ESIncStorageWrapper> esWrapperList) throws Exception {
        ArrayList<DocWriteRequest> bulkQuest = new ArrayList<>();

        for (ESIncStorageWrapper esWrapper : esWrapperList) {
            OperateType type = esWrapper.getType();
            //文档操作
            DocWriteRequest docWriteRequest = null;
            switch (type) {
                case DOCINSERT: {
                    log.debug("文档插入");
                    docWriteRequest = EsOperateUtil.DocumentAPI.insert(esWrapper.getIndicsName(), esWrapper.getDocumentId(), esWrapper.getDataMapping());
                    log.debug(docWriteRequest.toString());
                    break;
                }
                case DOCUPDATE: {
                    log.debug("文档更新");
                    docWriteRequest = EsOperateUtil.DocumentAPI.update(esWrapper.getIndicsName(), esWrapper.getDocumentId(), esWrapper.getDataMapping());
                    log.debug(docWriteRequest.toString());
                    break;
                }
                case DOCDELETE: {
                    log.debug("文档删除");
                    docWriteRequest = EsOperateUtil.DocumentAPI.delete(esWrapper.getIndicsName(), esWrapper.getDocumentId());
                    log.debug(docWriteRequest.toString());
                    break;
                }
                default: {
                    log.debug("未知文档操作[{}}", docWriteRequest.toString());
                }
            }
            bulkQuest.add(docWriteRequest);
        }
        //批量进行
        EsOperateUtil.bulk(client, bulkQuest);
    }


}
