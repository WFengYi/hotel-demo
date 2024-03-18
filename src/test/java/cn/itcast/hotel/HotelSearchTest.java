package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HotelSearchTest {
    private RestHighLevelClient client;

    @Test
    void testHighLight() throws IOException {

        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        //3.query
        request.source().query(QueryBuilders.matchQuery("name", "汉庭"));
        //3.高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //3.3添加分页

        //4.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析
        handleHightResponse(response);

    }

    /**
     * 聚合
     *
     * @throws IOException
     */
    @Test
    void testAggregation() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        //2.1 设置size
        request.source().size(0);
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")//参数聚合名字
                .field("brand")
                .size(20)
        );
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        System.out.println(response);
        //5.响应过来的聚合结果解析
        Aggregations aggregations = response.getAggregations();
        //6.根据名称获取聚合结果
        Terms brandTerms = aggregations.get("brandAgg");//参数里是聚合名字
        //7.获取桶
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            //获取key
            String brandName = bucket.getKeyAsString();
            System.out.println("brandName = " + brandName);
        }
    }


    /**
     * 自动补全
     *
     * @throws IOException
     */
    @Test
    void testAutoComplete() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source()
                .suggest(new SuggestBuilder().addSuggestion(
                        "mySuggestion",
                        SuggestBuilders.completionSuggestion("suggestion")
                                .prefix("h")//要自动补全的拼音
                                .skipDuplicates(true)
                                .size(10)
                ));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestion = suggest.getSuggestion("mySuggestion");
        for (CompletionSuggestion.Entry.Option options : suggestion.getOptions()) {
            String text = options.getText().string();
            if (text.contains("/")){
                text = text.replace("/", "\n");
            }
            System.out.println(text);
        }
    }

    @Test
    void testPageAndSort() throws IOException {
        int page = 2, size = 10;
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        //3.query
        request.source().query(QueryBuilders.matchAllQuery());
        //3.2添加排序
        request.source().sort("price", SortOrder.DESC);
        //3.3添加分页
        request.source().from((page - 1) * size).size(10);
        //4.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析
        handleResponse(response);

    }

    @Test
    void testBool() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        //2.1准备booleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2添加条件
        boolQuery.must(QueryBuilders.termQuery("city", "北京"));
        boolQuery.mustNot(QueryBuilders.termQuery("city", "上海"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(200));
        //3.发送请求
        request.source().
                query(boolQuery);
        //3.执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析
        handleResponse(response);

    }


    @Test
    void testMatch() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        request.source().
                query(QueryBuilders.matchQuery("name", "汉庭"));
        //3.执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析
        handleResponse(response);

    }


    @Test
    void testMatchAll() throws IOException {
        //1.准备Request对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL语句
        request.source().
                query(QueryBuilders.matchAllQuery());
        //3.执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析
        handleResponse(response);

    }

    /**
     * 解析响应结果
     *
     * @param response
     */
    private static void handleResponse(SearchResponse response) {
        //4.解析响应
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        System.out.println("总共搜索到 " + total + " 条数据");
        //文档总数
        SearchHit[] hits = searchHits.getHits();
        //5.解析
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            System.out.println(hotelDoc);
        }
    }

    /**
     * @param response
     */
    private void handleHightResponse(SearchResponse response) {
        //4.解析响应
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        System.out.println("总共搜索到 " + total + " 条数据");
        //文档总数
        SearchHit[] hits = searchHits.getHits();
        //5.解析
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                //获取高亮字段内容
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    //数组下标为0就是name高亮字段的值
                    String name = highlightField.getFragments()[0].toString();
                    //设置name字段内容
                    hotelDoc.setName(name);
                }
            }
            System.out.println(hotelDoc);
        }
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.7.136:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
