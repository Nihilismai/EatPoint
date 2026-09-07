package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Resource
    private RedissonClient redissonClient;



    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    //订单队列
//    private BlockingQueue<VoucherOrder> orderTask = new ArrayBlockingQueue<>(1024+1024);

    //秒杀订单线程池
    private static final ExecutorService SECKILL_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_EXECUTOR.submit(new VoucherOrderHandler());
    }


    private void createConsumerGroup() {
        try {
            // 检查消费者组是否已存在
            StreamOperations<String, Object, Object> streamOps = stringRedisTemplate.opsForStream();

            // 尝试创建消费者组，如果不存在则创建
            streamOps.createGroup(queueName, "g1");
        } catch (Exception e) {
            // 如果消费者组已存在，会抛出异常，忽略即可
            log.info("消费者组 g1 已存在，无需重复创建");
        }
    }

    String queueName = "stream.orders";

    private class VoucherOrderHandler implements Runnable {



        @PostConstruct
        private void init() {
            // 创建消费者组
            createConsumerGroup();

            // 启动主消费者
            SECKILL_EXECUTOR.submit(new VoucherOrderHandler());

            // 启动 pending 消息处理（你代码里已经有了，但需要启动）
            // SECKILL_EXECUTOR.submit(() -> new VoucherOrderHandler().handlePendinglist());
        }





        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断是否成功，如果失败则继续循环
                    if (list == null || list.isEmpty()) {
                        continue;
                    }

                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //创建订单
                    handleOrder(voucherOrder);

                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());


                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendinglist();

                }
            }
        }
        private void handlePendinglist() {
            while (true) {
                try {
                    //获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断是否成功，如果失败则继续循环
                    if (list == null || list.isEmpty()) {
                        break;
                    }

                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //创建订单
                    handleOrder(voucherOrder);

                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());


                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    try{
                        Thread.sleep(2000);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }


        private void handleOrder (VoucherOrder voucherOrder) {
            //1.获取订单信息
            Long userId = voucherOrder.getUserId();
            Long voucherId = voucherOrder.getVoucherId();
            System.out.println("处理订单：" + userId + " " + voucherId);
            //创建对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //2.尝试获取锁
            boolean isLocked = lock.tryLock();
            if (!isLocked) {
                //3.获取锁失败，返回失败结果
                return;
            }
            try {
                //4.获取锁成功，创建订单
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                //5.释放锁
                lock.unlock();
            }
        }
    }

    private IVoucherOrderService proxy;

    //秒杀优惠券
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //创建订单
        Long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate
                .execute(SECKILL_SCRIPT,
                        Collections.emptyList(),
                        voucherId.toString(),
                        userId.toString(),
                        String.valueOf(orderId)
        );

        //判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }



        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();



        //返回订单id
        return Result.ok(orderId);
    }

    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();

        if(count >0){
            //用户已经购买过了
            return;
        }

        boolean success = iSeckillVoucherService.update(
                new UpdateWrapper<SeckillVoucher>()
                        .eq("voucher_id", voucherOrder.getVoucherId())
                        .gt("stock", 0)
                        .setSql("stock = stock - 1")
        );

        if (!success) {
            //扣减失败
            return;
        }
        save(voucherOrder);
    }


    /**
     * 创建秒杀订单
     * @param userId 用户ID
     * @param voucherId 优惠券ID
     * @return 订单ID
     */
    @Transactional
    public Long createOrder(Long userId, Long voucherId) {

        VoucherOrder voucherOrder = new VoucherOrder();
        //获取订单id（全局唯一id）
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        voucherOrder.setUserId(userId);
        //代金券id
        voucherOrder.setVoucherId(voucherId);
        //保存订单
        save(voucherOrder);

        return orderId;
    }




}

