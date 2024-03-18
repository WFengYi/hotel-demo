package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            //1.准备Request对象
            SearchRequest request = new SearchRequest("hotel");
            builderBasicQuery(params,request);

            //2.2分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);

            //2.3排序
            String location = params.getLocation();
            if (location != null && !location.equals("")){
                request.source().sort(SortBuilders
                        .geoDistanceSort("location",new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }
            //3.执行请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //解析

            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    public Map<String, List<String>> filters(RequestParams params)  {
        try {
            SearchRequest request = new SearchRequest("hotel");
            //2.准备DSL语句
            //2.1 设置size
            builderBasicQuery(params,request);
            request.source().size(0);
            request.source().aggregation(AggregationBuilders
                    .terms("brandAgg")//参数聚合名字
                    .field("brand")
                    .size(20)
            );
            request.source().aggregation(AggregationBuilders
                    .terms("cityAgg")//参数聚合名字
                    .field("city")
                    .size(50)
            );
            request.source().aggregation(AggregationBuilders
                    .terms("starNameAgg")//参数聚合名字
                    .field("starName")
                    .size(50)
            );
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            //4.1.响应过来的聚合结果解析
            Aggregations aggregations = response.getAggregations();
            //过滤返回
            Map<String,List<String>> map = new HashMap<>();
            //4.2根据名称解析结果
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            List<String> starNameList = getAggByName(aggregations, "starNameAgg");
            map.put("品牌",brandList);
            map.put("城市",cityList);
            map.put("星级",starNameList);
            return map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestion(String key) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source()
                    .suggest(new SuggestBuilder().addSuggestion(
                            "mySuggestion",
                            SuggestBuilders.completionSuggestion("suggestion")
                                    .prefix(key)//要自动补全的拼音
                                    .skipDuplicates(true)
                                    .size(10)
                    ));
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestion = suggest.getSuggestion("mySuggestion");
            List<String> suggestionList = new ArrayList<>();
            for (CompletionSuggestion.Entry.Option options : suggestion.getOptions()) {
                String text = options.getText().string();
                if (text.contains("/")){
                    text = text.replace("/", "\n");
                }
                suggestionList.add(text);
            }
            return suggestionList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getAggByName(Aggregations aggregations, String aggName) {
        List<String> brandList = new ArrayList<>();
        //6.根据名称获取聚合结果
        Terms brandTerms = aggregations.get(aggName);//参数里是聚合名字
        //7.获取桶
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }

    private static void builderBasicQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        String key = params.getKey();
        //2.准备DSL语句
        //2.1关键词搜索
        if (StringUtils.isEmpty(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("name", key));
        }
        //2.2品牌搜索
        if(params.getBrand() != null && !params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        //2.3城市搜索
        if(params.getCity() != null && !params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        //2.3星级搜索
        if(params.getStarName() != null && !params.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        //2.3价格区间搜索
        if(params.getMinPrice() != null && params.getMaxPrice() != null ){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }
        //3.算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        //原始查询，相关性算分的分数
                        boolQuery,
                        //算分数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件
                                        QueryBuilders.termQuery("ADing", "true"),
                                        //算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQueryBuilder);
    }

    private static PageResult handleResponse(SearchResponse response) {
        //1.解析响应
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        System.out.println("总共搜索到 " + total + " 条数据");
        //2。文档数组
        SearchHit[] hits = searchHits.getHits();
        //集合存放文档
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            //3.解析数据 封装对象
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
            System.out.println(hotelDoc);
        }
        return new PageResult(total, hotels);
    }
}
