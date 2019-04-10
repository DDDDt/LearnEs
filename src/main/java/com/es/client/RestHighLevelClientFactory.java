package com.es.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author dt 2019/4/10 11:31
 * RestHighLevelClient 工厂类
 */
public class RestHighLevelClientFactory {

    public static RestHighLevelClient create(){
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("",9200,"http")
                )
        );
    }

}
