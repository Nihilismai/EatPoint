package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.baomidou.mybatisplus.core.toolkit.IdWorker.getId;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IUserService userService;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        if (isFollow) {
            Follow follow = new Follow()
                    .setUserId(userId)
                    .setFollowUserId(id);
            save(follow);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                stringRedisTemplate.opsForSet().add("follow:" + userId, id.toString());
            }
        } else {
            // 取消关注
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", id));
            if (isSuccess){
                stringRedisTemplate.opsForSet().remove("follow:" + userId, id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("follow_user_id", id).count();
        return Result.ok(count > 0 );
    }

    @Override
    public Result commonFriends(Long id) {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 获取两个用户关注的交集
        Set<String> followUserIds = stringRedisTemplate.opsForSet().intersect("follow:" + userId, "follow:" + id);
        if(followUserIds == null || followUserIds.isEmpty())
            return Result.ok();
        //解析id
        followUserIds.stream().map(s -> Long.valueOf(s)).collect(Collectors.toList());
        List<User> users = userService
                .listByIds(followUserIds.stream()
                .map(s -> Long.valueOf(s))
                        .collect(Collectors.toList()));
        return Result.ok(followUserIds);
    }
}
