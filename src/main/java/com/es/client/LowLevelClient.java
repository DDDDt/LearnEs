package com.es.client;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

/**
 * @author dt 2019/4/10 14:00
 */
public class LowLevelClient {

    private RestClient restClient;

    @Before
    public void getRestClient(){
        restClient = RestLowLevelClientFavtory.create();
    }

    @Test
    public void getEs() throws IOException, InterruptedException {
        // 同步
        Response result = restClient.performRequest("GET", "/");
        System.out.println(EntityUtils.toString(result.getEntity()));

        // 异步
        restClient.performRequestAsync("GET", "/", new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                System.out.println(response.getRequestLine().getUri());
                System.out.println(response.getHost().toHostString());
                System.out.println(response.getStatusLine().getStatusCode());
                try {
                    System.out.println(EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception exception) {
                System.out.println("失败"+exception.getMessage());
            }
        });

        Thread.sleep(1000);

    }

    @After
    public void close() throws IOException {
        if(Objects.nonNull(restClient)){
            restClient.close();
        }
    }

}
