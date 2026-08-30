package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_LIST_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        //使用list取 [0 -1] 代表全部
        //从redis中查询所有分类
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(SHOP_LIST_KEY, 0, -1);
        if (CollectionUtil.isNotEmpty(shopTypeList)) {
            //shopTypeList.get(0) 其实是获取了整个List集合里的元素,0是第0个key
            //查询到，直接返回
            List<ShopType> types = JSONUtil.toList(shopTypeList.get(0), ShopType.class);
            return Result.ok(types);
        }



        //redis中没有，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //数据库中没有，报错
        if (CollectionUtil.isEmpty(typeList)) {
            return Result.fail("列表信息不存在");
        }

        //list 存
        //数据库中有，存到redis，注意存的位置和上面取的位置一致。
        String jsonStr = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForList().leftPushAll(SHOP_LIST_KEY, jsonStr);

        return Result.ok(typeList);


    }
}
