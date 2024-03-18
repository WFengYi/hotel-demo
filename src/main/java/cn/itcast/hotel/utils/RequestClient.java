package cn.itcast.hotel.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RequestClient {

    @Bean
    public RestHighLevelClient client() {
        return new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.7.136:9200")
        ));
    }
}
