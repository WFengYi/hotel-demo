package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient client;

    @Test
    void testAddDocument() throws IOException {
        //根据ID查询酒店数据
        Hotel hotel = hotelService.getById(36934L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        //1.准备request对象 对象参数是索引库名
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //2.准备JSON文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("hotel", "36934");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //解析响应结果

        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * 修改文档
     * @throws IOException
     */
    @Test
    void testUpdateDocumentById() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel","36934");
        request.doc(
                "price","336",
                "starName","三钻"
        );
        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel","36934");

        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkDocument() throws IOException {
        List<Hotel> hotels = hotelService.list();

        //遍历赋值

        BulkRequest bulkRequest = new BulkRequest();
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
//            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
//            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
//            bulkRequest.add(indexRequest);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }


    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.7.132:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
