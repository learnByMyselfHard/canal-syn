package com.lai.canalsyn.client.elasticsearch.storage.impl;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @ Author : lai
 * @ Date   : created in  2020/6/24 14:05
 * @ Description :
 */
@Component
public class HelloWorldEsStorageSupport extends AbstractESStorageSupport {
    private String dbTableName = "helloworld";
    private Map scheme=null;
    @Override
    public String getTableName() {
        return dbTableName;
    }

    @Override
    public Map getScheme() {
        //懒加载索引结构,这里不需要加锁。
        if(scheme==null) {
            //后面考虑使用注解
            Map<String, Object> name = new HashMap<>();
            name.put("type","keyword");
            Map<String, Object> age = new HashMap<>();
            age.put("type","integer");
            Map<String, Object> desc = new HashMap<>();
            desc.put("type","text");
            Map<String, Object> length = new HashMap<>();
            length.put("type","integer");
            Map<String, Object> address = new HashMap<>();
            address.put("type","keyword");
            Map<String, Object> date = new HashMap<>();
            date.put("type","date");

            //组装
            Map<String, Object> properties = new HashMap<>();
            properties.put("name", name);
            properties.put("age", age);
            properties.put("desc", desc);
            properties.put("length", length);
            properties.put("address", address);
            properties.put("date", date);
            Map<String, Object> mapping = new HashMap<>();
            mapping.put("properties", properties);
            scheme=mapping;
        }
        return scheme;
    }
}
