package com.lai.canalsyn;

import com.lai.canalsyn.message.Dml;
import com.lai.canalsyn.plugin.CanalConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 *
 */
@Slf4j
@Component
public class DefaultCanalClientWork extends  AbstractCanalClientWork {

    @Autowired(required = false)
    List<CanalConsumer>  canalConsumers;

    @Override
    public void preProceed() {
        for (CanalConsumer canalConsumer : canalConsumers) {
            canalConsumer.init();
        }
    }

    @Override
    protected void internalWriteOut(List<Dml> dmls) {
        for (Dml dml : dmls) {
            String type = dml.getType();
            String operateString="";
            if(!CollectionUtils.isEmpty(canalConsumers)){
                for (CanalConsumer canalConsumer : canalConsumers) {
                    if (canalConsumer.support(dml)) {
                        switch (type) {
                            case "INSERT": {
                                operateString = "插入操作";
                                canalConsumer.insert(dml);
                                break;
                            }
                            case "UPDATE": {
                                operateString = "更新操作";
                                canalConsumer.update(dml);
                                break;
                            }
                            case "DELETE": {
                                operateString = "删除操作";
                                canalConsumer.delete(dml);
                                break;
                            }
                            default: {
                                log.info("不支持的操作[{}]", type);
                            }
                        }
                        log.info(operateString);
                        log.info(" message:[{}]", dml);
                    }
                }
            }
        }
    }
}