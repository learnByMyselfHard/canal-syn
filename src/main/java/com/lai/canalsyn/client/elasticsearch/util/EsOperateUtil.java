package com.lai.canalsyn.client.elasticsearch.util;


import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/21 10:08
 * @ Description :  ES    DDL工具
 */
@Slf4j
@Data
public class EsOperateUtil {
    //索引缓存
    private static ConcurrentHashMap<String, Integer> indicsExistCache = new ConcurrentHashMap();


    public static ConcurrentHashMap<String, Integer> getIndicsExistCache() {
        return indicsExistCache;
    }

    @Slf4j
    public static class IndexAPI {
        //创建索引  加入缓存机制
        public static boolean createIndex(RestHighLevelClient client, String indics, Set<String> aliases, Map<String, Object> scheme) {
            if (scheme != null && indicsExistCache.get(indics) == null) {
                indicsExistCache.computeIfAbsent(indics, (x) -> {
                    if (!indexIsExist(client, indics)) {
                        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indics);
                        createIndexRequest.mapping(scheme);
                        int size=1;
                        createIndexRequest.settings(Settings.builder()
                                .put("index.number_of_shards", size)
                                .put("index.number_of_replicas", size - 1)
                                .build());
                        //添加别名
                        if (aliases != null) {
                            for (String a : aliases) {
                                createIndexRequest.alias(new Alias(a));
                            }
                        }
                        CreateIndexResponse response = null;
                        try {
                            response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        if (response.isAcknowledged()) {
                            log.info("创建的索引配置信息为[{}]：", createIndexRequest.settings());
                            log.info("成功创建索引[{}] aliases:", indics, JSON.toJSONString(aliases));
                        }

                        return Integer.valueOf(1);
                    }
                    return Integer.valueOf(2);
                });
                if (indicsExistCache.get(indics).equals(2)) {
                    return false;
                } else {
                    return true;
                }

            } else {
                return false;
            }

        }

        //查看索引是否存在
        public static boolean indexIsExist(RestHighLevelClient client, String indics) {
            GetIndexRequest getIndexRequest = new GetIndexRequest(indics);
            try {
                return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.info("判断索引是否存在的异常信息{}", e.getMessage());
                e.printStackTrace();
            }
            return true;
        }
    }

    @Slf4j
    public static class DocumentAPI {
        //构建创建单个文档请求
        public static IndexRequest insert(String indics, String documentId, Map insertMessageMap) {
            return new IndexRequest(indics)
                    .id(documentId).source(insertMessageMap)
                    ;
        }
        public static IndexRequest insert(String indics, String documentId, String insertMessageJson) {
            return new IndexRequest(indics)
                    .id(documentId).source(insertMessageJson)
                    ;
        }

        //根据文档id删除
        public static DeleteRequest delete(String indics, String documentId) {
            return new DeleteRequest(indics)
                    .id(documentId);
        }

        //构建更新单个文档请求
        public static UpdateRequest update(String indics, String documentId, Map updateMessage) {
            return new UpdateRequest(
                    indics,
                    documentId).doc(updateMessage);
        }
    }


    /**
     * ES的bulk提交对数据包大小有限制
     *
     * @param client
     * @param docWriteRequestList
     */
    //批量操作
    public static void bulk(RestHighLevelClient client, List<DocWriteRequest> docWriteRequestList) {
        if (!docWriteRequestList.isEmpty()) {
            BulkRequest bulkRequest = new BulkRequest();
            //操作后可见
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            for (DocWriteRequest docWriteRequest : docWriteRequestList) {
                bulkRequest.add(docWriteRequest);
            }
            try {
                BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (response.hasFailures()) {
                    log.error("失败处理的数据请求为 error:{}",
                            response.buildFailureMessage());
                } else {
                    log.info("成功处理[{}]条数据", bulkRequest.requests().size());
                }
            } catch (IOException e) {
                log.error("批量处理数据到ES失败:", e);
            }
        }

    }


    //API测试
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                ));
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        HashMap<String, Object> keyword = new HashMap<>();
        keyword.put("type", "keyword");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        properties.put("keyword", keyword);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        //索引API
        //EsOperateUtil.IndexAPI.createIndex(client,"wudi",null,mapping);
        //System.out.println(EsOperateUtil.IndexAPI.indexIsExist(client, "wudi"));
        //文档API
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kiasdasdmchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        //插入文档测试
//        IndexRequest indexRequest = EsOperateUtil.DocumentAPI.insert("wudi", ESWrapper.builder().documentId("1").source(jsonMap).build());
//        client.index(indexRequest,RequestOptions.DEFAULT);
        //删除文档测试
        DeleteRequest deleteRequest = DocumentAPI.delete("wudi", "1");
        client.delete(deleteRequest, RequestOptions.DEFAULT);
        //更新文档测试
        Map<String, Object> updateMap = new HashMap<>();
        jsonMap.put("user", "yoyoyoyo");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "111111!!");
        UpdateRequest updateRequest = DocumentAPI.update("wudi", "1", jsonMap);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.getGetResult());
    }

}
