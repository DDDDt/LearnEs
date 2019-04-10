package com.es.client;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author dt 2019/4/10 17:49
 * 构建 es 搜索条件
 */
public class SearchEsRequest {

    /**
     * 查询所有条件
     * @return
     */
    public SearchRequest getAll(){
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        searchRequest.source(query);
        return searchRequest;
    }

    /**
     * 查询某个索引的某些条件
     * @return
     */
    public SearchRequest getIndexSearch(){
        SearchRequest searchRequest = new SearchRequest("test-esdt");
        searchRequest.types("external");
        // 添加搜索参数, 为搜索请求 body 中的所有内容提供了 setter
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("address","Terrace"))
                .from(0)
                .size(5)
                // 设置超时
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    /**
     * 模糊匹配
     * @return
     */
    public SearchRequest getIndexMatch(){
        SearchRequest searchRequest = new SearchRequest("test-esdt");
        searchRequest.types("external");
        // 添加搜索参数, 为搜索请求 body 中的所有内容提供了 setter
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("address", "Terrace")
               // 启用模糊匹配
                .fuzziness(Fuzziness.AUTO)
                // 在匹配查询上设置前缀长度选项
                .prefixLength(3)
                // 设置最大扩展选项以控制查询的模糊过程
                .maxExpansions(10);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(matchQueryBuilder)
                .from(0)
                .size(5)
                // 设置超时
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    /**
     * 高亮
     * @return
     */
    public SearchRequest getHighLightEsData(){
        SearchRequest searchRequest = new SearchRequest("test-esdt");
        searchRequest.types("external");
        // 添加搜索参数, 为搜索请求 body 中的所有内容提供了 setter
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("address", "Terrace")
                // 启用模糊匹配
                .fuzziness(Fuzziness.AUTO)
                // 在匹配查询上设置前缀长度选项
                .prefixLength(3)
                // 设置最大扩展选项以控制查询的模糊过程
                .maxExpansions(10);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(matchQueryBuilder)
                .from(0)
                .size(5)
                // 设置超时
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("address");
        highlightBuilder.highlighterType("unified");
        highlightBuilder.field("firstname");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

}
