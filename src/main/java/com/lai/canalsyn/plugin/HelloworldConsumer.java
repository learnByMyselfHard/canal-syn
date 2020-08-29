package com.lai.canalsyn.plugin;

import com.lai.canalsyn.message.Dml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @ Author : lai
 * @ Date   : created in  2020/8/29 14:44
 * @ Description :
 */
@Component
@Slf4j
public class HelloworldConsumer implements CanalConsumer {
    @Override
    public boolean support(Dml dml) {
        return true;
    }
    @Override
    public void insert(Dml dml) {
        log.info("helloworld 插入");
    }

    @Override
    public void update(Dml dml) {
        log.info("helloworld 更新");
    }

    @Override
    public void delete(Dml dml) {
        log.info("helloworld 删除");
    }

    @Override
    public void init() {
        log.info("helloworld 初始化");
    }

    ;
}
