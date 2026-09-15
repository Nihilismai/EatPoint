package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.lang.reflect.Executable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService ex = Executors.newFixedThreadPool(500);
    @Autowired
    private RedisIdWorker redisIdWorker;

    @Test
    void contextLoads() {
        shopService.saveShop2Redis(1L, 20L);
    }

    @Test
    void testIdWorker(){
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
        for(int i = 0; i < 100; i++){
            Long id = redisIdWorker.nextId("order");
            System.out.println(id);
         }
        latch.countDown();
        };
        Long startTime = System.currentTimeMillis();
        for(int i = 0; i < 300; i++){
            ex.submit(task);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Long endTime = System.currentTimeMillis();
        System.out.println("time: " + (endTime - startTime));
    }


    @Test
    void loadShopData(){
        //查询店铺信息
        List<Shop> shops = shopService.list();
        //将店铺分组，按照typeId分组，type一致的放到一个集合
        Map<Long,List<Shop>> shopMap = shops.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        for(Map.Entry<Long, List<Shop>> entry : shopMap.entrySet()){
        //获取类型id
            Long typeId = entry.getKey();
            //获取类型下的店铺列表
            List<Shop> values = entry.getValue();

            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
            //写入redis
            for (Shop shop : values) {
                locations.add(new RedisGeoCommands
                        .GeoLocation<>(shop.getId()
                        .toString(),
                        new Point(shop.getX(), shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add("shop:geo:" + typeId, locations);
        }
    }

}
