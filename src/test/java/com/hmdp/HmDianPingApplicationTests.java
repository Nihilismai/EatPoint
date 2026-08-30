package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.lang.reflect.Executable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;


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

}
