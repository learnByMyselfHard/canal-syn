package com.lai.canalsyn;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/17 16:34
 * @ Description :  canal客户端
 */
@Component
@Slf4j
public class CanalClient {
    @Resource
    private AbstractCanalClientWork clientWork;

    @PostConstruct
    public synchronized void init() {
        try {
            log.info("canl客户端线程启动!!");
            //开启工作线程
            clientWork.start();
            log.info("running now ......");
        } catch (Exception e) {
            log.error("goes wrong when starting up the canal client ", e);
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        System.out.println("canl客户端线程销毁！！");
    }

}
