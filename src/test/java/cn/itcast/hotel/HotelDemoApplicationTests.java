package cn.itcast.hotel;

import cn.itcast.hotel.service.impl.HotelService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDemoApplicationTests {
    @Autowired
    private HotelService hotelService;

    @Test
    void contextLoads() {

        Map<String, List<String>> filters = hotelService.filters();
        System.out.println("filters = " + filters);
    }

}
