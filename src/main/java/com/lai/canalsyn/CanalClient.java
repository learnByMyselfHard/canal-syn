package com.lai.canalsyn;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
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
public class CanalClient implements CommandLineRunner {
    @Resource
    private AbstractCanalClientWork clientWork;
    @PreDestroy
    public synchronized void destroy() {
        System.out.println("canl客户端线程销毁！！");
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            log.info("canl客户端线程启动!!");
            //开启工作线程
            clientWork.start();
            log.info("running now ......");
        } catch (Exception e) {
            log.error("goes wrong when starting up the canal client ", e);
        }
    }
}
