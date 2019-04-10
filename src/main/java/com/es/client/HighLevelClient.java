package com.es.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author dt 2019/4/10 14:56
 * 高等级 es 库
 */
public class HighLevelClient {

    private RestHighLevelClient restHighLevelClient;

    @Before
    public void getClient(){
        restHighLevelClient = RestHighLevelClientFactory.create();
    }

    @Test
    public void getEsBaseInfo() throws IOException {

        MainResponse info = restHighLevelClient.info();
        System.out.println(info.getClusterName().toString());

    }

    // 索引
    @Test
    public void getEsIndex() throws InterruptedException {
        // 文档
        Map<String,Object> sourceMap = new HashMap<>();
        sourceMap.put("user","dt");
        sourceMap.put("postDate",new Date());
        sourceMap.put("message","测试通过 java api 创建插入数据");

        IndexRequest request = new IndexRequest("test-esdt","javaAPI","1")
                .source(sourceMap);
        restHighLevelClient.indexAsync(request,new ActionListener<IndexResponse>(){
            @Override
            public void onResponse(IndexResponse indexResponse) {

                System.out.println(indexResponse.toString());

            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("查询索引出现错误,e = "+e.getMessage());
            }
        });

        Thread.sleep(1000);

    }

    //  get 获取值
    @Test
    public void getEsData() throws InterruptedException {

        GetRequest request = new GetRequest("test-esdt", "external", "1");
        restHighLevelClient.getAsync(request, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse documentFields) {
                System.out.println(documentFields.getSourceAsString());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("查询数据失败,e = "+e.getMessage());
            }
        });

        Thread.sleep(1000);

    }

    // exists 请求
    @Test
    public void existsEs() throws InterruptedException {
        GetRequest request = new GetRequest("test-esdt", "external", "1");
        // 禁用提取 _source
        request.fetchSourceContext(new FetchSourceContext(false));
        // 禁用提取存储的字段
        request.storedFields("_none_");
        restHighLevelClient.existsAsync(request, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                System.out.println(aBoolean.booleanValue());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("判断存不存在出错, e = "+e.getMessage());
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void deleteEsDate() throws InterruptedException {

        DeleteRequest request = new DeleteRequest("test-esdt", "external", "1");
        restHighLevelClient.deleteAsync(request, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                System.out.println(deleteResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("删除 es 数据错处");
            }
        });

        Thread.sleep(1000);

    }

    @After
    public void close() throws IOException {
        if(Objects.nonNull(restHighLevelClient)){
            restHighLevelClient.close();
        }
    }

}
