package com.example;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ESManager {

    private RestHighLevelClient client;

    private ESManager(Map<String, Integer> config) {
        List<HttpHost> list = new ArrayList<>();
        for (Map.Entry<String, Integer> item : config.entrySet()) {
            list.add(new HttpHost(item.getKey(), item.getValue()));
        }
        RestClientBuilder clientBuilder = RestClient.builder(list.toArray(new HttpHost[]{}));
        client = new RestHighLevelClient(clientBuilder);
    }

    public static ESManager getInstance(Map<String, Integer> config) {
        return new ESManager(config);
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public Map<String, DocumentField> getFieldsById(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        return response.getFields();
    }

    public DocumentField getFieldById(String index, String type, String id, String fieldName) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        return response.getField(fieldName);
    }

    public Map<String, Object> getSourceById(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        return response.getSource();
    }

    public String getSourceAsStringById(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        return response.getSourceAsString();
    }

    public long getVersionById(String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        return response.getVersion();
    }

    public SearchResponse searchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchHit[] searchHitsAll() throws IOException {
        return searchAll().getHits().getHits();
    }

    public SearchResponse searchFromSize(int from, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(from).size(size);
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchHit[] searchHitsFromSize(int from, int size) throws IOException {
        return searchFromSize(from, size).getHits().getHits();
    }

    public SearchResponse searchQuery(QueryBuilder builder) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(builder);
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse searchQueryFromSize(QueryBuilder builder, int from, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(builder).from(from).size(size);
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public List<SearchHit> searchScrollQuerySize(String index, long second, SearchSourceBuilder builder) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        if (builder != null) {
            searchRequest.source(builder);
        }
        searchRequest.scroll(TimeValue.timeValueSeconds(second));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        final List<SearchHit> list = new ArrayList<SearchHit>(Arrays.asList(hits));
        String scrollId = searchResponse.getScrollId();
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
        SearchResponse scroll = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
        list.addAll(Arrays.asList(scroll.getHits().getHits()));
        return list;
    }

    /**
     * 不建议使用，测试用的api 原因：es插入非常缓慢，需要bulk的方式批量插入
     *
     * @param index  索引
     * @param type   类型
     * @param id     id
     * @param source json数据
     * @return status
     * @throws IOException
     */
    private int index(String index, String type, String id, String source) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index).type(type).id(id).timeout(TimeValue.timeValueSeconds(2));
        indexRequest.source(source, XContentType.JSON);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        return response.status().getStatus();
    }

    public boolean exist(String index) throws IOException {
        IndicesClient indices = client.indices();
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        return indices.exists(request, RequestOptions.DEFAULT);
    }

    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
