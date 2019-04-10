package com.es.client;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * @author dt 2019/4/10 13:34
 * es 低级别客户端
 */
public class RestLowLevelClientFavtory {

    public static RestClient create(){
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("", 9200, "http")
        );

        // 设置请求头
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);

        // 设置多次尝试统一请求时应该遵守的超时时间
        builder.setMaxRetryTimeoutMillis(10000);

        // 设置错误监听
        builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(HttpHost host) {
                System.out.println("错误"+host.toHostString());
            }
        });

        return builder.build();
    }

}
