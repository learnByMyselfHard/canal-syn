package com.lai.canalsyn.client.elasticsearch;


import com.lai.canalsyn.AbstractCanalClientWork;
import com.lai.canalsyn.config.CanalClientConfig;
import com.lai.canalsyn.client.elasticsearch.config.EsCanalClientConfig;
import com.lai.canalsyn.client.elasticsearch.storage.ESEtlManager;
import com.lai.canalsyn.client.elasticsearch.storage.ESIncStorageManager;
import com.lai.canalsyn.message.Dml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/17 16:38
 * @ Description :  基于canal实现ES实时数据同步
 */
@Slf4j
@Component
public class EsCanalClientWork extends AbstractCanalClientWork {
    //默认配置
    private static final int ES_BATCH_SIZE = 50;
    @Resource
    ESEtlManager esEtlManager;
    @Resource
    ESIncStorageManager esIncStorageManager;

    private EsCanalClientConfig esCanalClientConfig;


    /**
     * 增量的前置处理,在这里就是全量同步
     */
    @Override
    public void preProceed() {
        super.preProceed();
        buildEsSynConfig();
        try {
            esEtlManager.doFullStorage();
        } catch (IOException e) {
            log.debug(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 配置构建
     */
    public void buildEsSynConfig() {
        CanalClientConfig canalClientConfig = getCanalClientConfig();
        EsCanalClientConfig esCanalClientConfig = new EsCanalClientConfig();
        this.esCanalClientConfig = esCanalClientConfig;

    }

    @Override
    protected void internalWriteOut(List<Dml> dmls) {
        try {
            esIncStorageManager.doIncStorage(dmls);
        } catch (IOException e) {
            log.debug(e.getMessage());
            e.printStackTrace();
        }
    }
}
