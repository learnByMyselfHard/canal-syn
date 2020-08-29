package com.lai.canalsyn;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.lai.canalsyn.config.CanalClientConfig;
import com.lai.canalsyn.message.Dml;
import com.lai.canalsyn.util.MessageUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/17 17:20
 * @ Description : canal增量消费模板
 */
@Data
@Slf4j
public abstract class AbstractCanalClientWork {
    //canal配置
    @Resource
    private CanalClientConfig canalClientConfig;
    //线程
    protected Thread thread = null;
    //异常处理
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> log.error("parse events has an error", e);
    //连接
    protected CanalConnector connector;
    protected volatile boolean running = false;                                                 // 是否运行中

    private final void init() {
        log.info("canal client init!!");
        String destination = canalClientConfig.getDestination();
        String username = canalClientConfig.getUsername();
        String password = canalClientConfig.getPassword();
        //连接方式
        String canalServerHost = canalClientConfig.getCanalServerHost();
        String zookeeperHosts = canalClientConfig.getZookeeperHosts();
        if (!zookeeperHosts.trim().isEmpty()) {
            log.info("集群canal server");
            connector = CanalConnectors.newClusterConnector(zookeeperHosts, destination, username, password);
        }
        log.info("单机canal server");
        String[] split = canalServerHost.trim().split(":");
        String ip = split[0];
        int port = Integer.valueOf(split[1]);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,
                port), destination, username, password);

    }

    //canal客户端初始化启动
    public void start() {
        init();
        thread = new Thread(this::proceed, "canal-syn");
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    //转换成DML对象
    protected void writeOut(final Message message) {
        //消息转换
        List<Dml> dmls = MessageUtil.parse4Dml(canalClientConfig.getDestination(), message);
        for (Dml dml : dmls) {
            log.debug(dml.toString());
        }
        //子类需要重写
        internalWriteOut(dmls);
    }

    /**
     * 子类介入
     */
    public void preProceed() {
    }

    public void proceed() {
        preProceed();
        int emptyCount = 0;
        while (!running) { // waiting until running == true
            while (!running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
        int retry = canalClientConfig.getRetries() == null || canalClientConfig.getRetries() == 0 ? 1 : canalClientConfig.getRetries();
        if (retry == -1) {
            // 重试次数-1代表异常时一直阻塞重试
            retry = Integer.MAX_VALUE;
        }
        Integer batchSize = canalClientConfig.getBatchSize();
        if (batchSize == null) {

        }
        while (running) {
            try {
                log.info("=============> Start to connect destination: {} <=============", this.canalClientConfig.getDestination());
                connector.connect();
                log.info("=============> Start to subscribe destination: {} <=============", this.canalClientConfig.getDestination());

                //这里要和mysql的订阅最好一致不然有bug  // 指定filter，格式 {database}.{table}，这里不做过滤，过滤操作留给用户
                connector.subscribe(".*\\..*");
                log.info("=============> Subscribe destination: {} succeed <=============", this.canalClientConfig.getDestination());
                while (running) {

                    if (!running) {
                        break;
                    }

                    for (int i = 0; i < retry; i++) {
                        if (!running) {
                            break;
                        }
                        Message message = connector.getWithoutAck(batchSize);

                        long batchId = message.getId();
                        try {
                            int size = message.getEntries().size();
                            if (batchId == -1 || size == 0) {
                                //没有数据进行睡眠
                                log.info("empty count : " + ++emptyCount);
                                Thread.sleep(500);
                            } else {
                                emptyCount = 0;
                                if (log.isDebugEnabled()) {
                                    log.debug("destination: {} batchId: {} batchSize: {} ",
                                            this.canalClientConfig.getDestination(),
                                            batchId,
                                            size);
                                }
                                long begin = System.currentTimeMillis();
                                //消费
                                writeOut(message);
                                if (log.isDebugEnabled()) {
                                    log.debug("destination: {} batchId: {} elapsed time: {} ms",
                                            this.canalClientConfig.getDestination(),
                                            batchId,
                                            System.currentTimeMillis() - begin);
                                }
                            }
                            connector.ack(batchId); // 提交确认
                            break;
                        } catch (Exception e) {
                            if (i != retry - 1) {
                                connector.rollback(batchId); // 处理失败, 回滚数据
                                log.error(e.getMessage() + " Error sync and rollback, execute times: " + (i + 1));
                            } else {
                                connector.ack(batchId);
                                e.printStackTrace();
                                log.error(e.getMessage() + " Error sync but ACK!");
                            }
                            Thread.sleep(500);
                        }
                    }
                }

            } catch (Throwable e) {
                log.error("process error!", e);
            } finally {
                connector.disconnect();
                log.info("=============> Disconnect destination: {} <=============", this.canalClientConfig.getDestination());
            }

            if (running) { // is reconnect
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

    }

    public final CanalClientConfig getCanalClientConfig() {
        return canalClientConfig;
    }


    //具体子类实现
    protected abstract void internalWriteOut(List<Dml> dmls);


}
