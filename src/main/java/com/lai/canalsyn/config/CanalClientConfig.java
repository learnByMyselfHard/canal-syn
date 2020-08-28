package com.lai.canalsyn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/17 16:39
 * @ Description :  canal客户端相关配置
 */
@Component
@ConfigurationProperties(prefix = "canal.conf")
@Data
public class CanalClientConfig {

    // 批大小
    private Integer batchSize;
    // 同步分批提交大小
    private Integer syncBatchSize = 1000;
    // 重试次数
    private Integer retries=5;
    // 消费超时时间
    private Long timeout;


    // 单机模式下canal server的ip:port
    private String canalServerHost="localhost:1111";
    // 集群模式下的zk地址,如果配置了单机地址则以单机为准!!
    private String zookeeperHosts="localhost:2181";

    private  String destination="example";

    private  String username="root";
    private  String password="root";

}
