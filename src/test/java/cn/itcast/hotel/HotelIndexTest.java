package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constans.HotelConstants.MAPPING_TEMPLATE;

public class HotelIndexTest {
    private RestHighLevelClient client;

    @Test
    void esTest() {
        System.out.println("client = " + client);
    }

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    void creatIndexTest() throws IOException {
        //1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.请求参数，内容是创建索引库的DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引库
     * @throws IOException
     */
    @Test
    void deleteIndexTest() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");

        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引库是否存在
     * @throws IOException
     */

    @Test
    void existsIndexTest() throws IOException {
        //1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("hotel");

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists ?"索引库存在":"索引库不存在");
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
