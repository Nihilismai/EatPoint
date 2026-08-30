package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_ORDER_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_ORDER_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //获取当前登录用户（必须在任何可能提前返回的逻辑之前获取）
        Long userId = UserHolder.getUser().getId();

        //查询优惠券，判断是否存在（防止秒杀券不存在时空指针）
        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在");
        }

        //判断秒杀是否在规定时间（先判空，避免秒杀时间未配置时空指针）
        LocalDateTime now = LocalDateTime.now();
        if (voucher.getBeginTime() != null && voucher.getBeginTime().isAfter(now)) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime() != null && voucher.getEndTime().isBefore(now)) {
            return Result.fail("秒杀已经结束");
        }

        //库存是否充足（快速失败，最终以乐观锁扣减结果为准）
        if (voucher.getStock() == null || voucher.getStock() <= 0) {
            return Result.fail("库存不足");
        }

        //一人一单：基于Redis分布式锁做集群环境下的粗粒度校验，防止同一用户并发重复下单
        String lockKey = LOCK_ORDER_KEY + userId + ":" + voucherId;
        boolean isLock = tryLock(lockKey);
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            //数据库层面校验一人一单，防止该用户历史重复下单（事务内校验+插入，保证原子性）
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                return Result.fail("您已购买过该优惠券，不能重复下单");
            }

            //乐观锁扣减库存：带上 stock > 0 条件，并发下库存为0时CAS失败，防止超卖
            boolean success = iSeckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!success) {
                return Result.fail("库存不足");
            }

            //创建订单（与扣库存在同一事务中，要么同时成功，要么同时回滚）
            VoucherOrder voucherOrder = new VoucherOrder();
            //设置订单信息
            //获取订单id（全局唯一id）
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            //用户id
            voucherOrder.setUserId(userId);
            //代金券id
            voucherOrder.setVoucherId(voucherId);
            //保存订单
            save(voucherOrder);

            //返回订单id
            return Result.ok(orderId);
        } finally {
            //释放分布式锁（只释放本线程刚加的锁）
            unlock(lockKey);
        }
    }

    //尝试获取分布式锁（原子性加锁，带过期时间防止死锁）
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_ORDER_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放分布式锁（只释放本线程刚加的锁）
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
