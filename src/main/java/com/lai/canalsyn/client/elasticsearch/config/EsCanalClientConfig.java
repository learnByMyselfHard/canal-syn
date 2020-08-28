package com.lai.canalsyn.client.elasticsearch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @ Author : lai
 * @ Date   : created in  2020/6/24 12:21
 * @ Description :
 */
@Component
@ConfigurationProperties(prefix = "canal.es.conf")
public class EsCanalClientConfig {

}
