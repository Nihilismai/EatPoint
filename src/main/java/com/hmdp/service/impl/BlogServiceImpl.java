package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IBlogService blogService;
    @Autowired
    private IUserService userService;
    @Autowired
    private IFollowService followService;


    //分页查询
    @Override
    public Object queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return records;
    }

    @Override
    public Object queryBlogById(Long id) {
        // 根据id查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在");
        }
        //查询blog用户
        queryBlogUser(blog);
        //查询blog是否被点赞
        isBlogLiked(blog);
        return blog;
    }

    // 点赞功能
    @Override
    public Result likeBlog(Long id) {
        //获取登录用户
        UserDTO user = UserHolder.getUser();
        //判断当前用户是否已经点赞
        String key = "blog:like:" + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, user.getId().toString());
        if (score == null) {
            //如果未点赞，则点赞
            //更新数据库
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
            stringRedisTemplate.opsForZSet().add(key, user.getId().toString(), System.currentTimeMillis());
            }
        } else {
            //如果已经点赞，则取消点赞
            //更新数据库
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
            stringRedisTemplate.opsForZSet().remove(key, user.getId().toString());
            }
        }



        return Result.ok();
    }

    private static final int TOP_USER_COUNT = 10;

    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;

        // 1. 查询 Top N 用户 ID
        Set<String> topUserIds = stringRedisTemplate.opsForZSet()
                .range(key, 0, TOP_USER_COUNT - 1);

        if (topUserIds == null || topUserIds.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // 2. 解析用户 ID
        List<Long> ids = topUserIds.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());

        // 3. 批量查询用户
        List<User> users = userService.listByIds(ids);
        if (users == null || users.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // 4. 构建用户映射，保持顺序
        Map<Long, User> userMap = users.stream()
                .collect(Collectors.toMap(User::getId, Function.identity()));

        // 5. 按 ZSet 顺序转换 DTO
        List<UserDTO> userDTOs = ids.stream()
                .map(userMap::get)
                .filter(Objects::nonNull)
                .map(user -> {
                    UserDTO userDTO = new UserDTO();
                    BeanUtils.copyProperties(user, userDTO);
                    return userDTO;
                })
                .collect(Collectors.toList());

        return Result.ok(userDTOs);
    }

    //推送和保存笔记
    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存摊点笔记
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("保存笔记失败");
        }
        //查询笔记作者的所有粉丝
        List<Follow> fans = followService.query().eq("follow_user_id", user.getId()).list();
        //推送笔记id给所有粉丝
        fans.forEach(fan -> {
            stringRedisTemplate.opsForZSet().add("feed:" + blog.getId(), blog.getId().toString(), System.currentTimeMillis());
        });


        //返回id

        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        //查询收件箱
        String key = FEED_KEY + user.getId();
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        // 判断数据是否存在
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //解析数据
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //获取id
            ids.add(Long.valueOf(tuple.getValue()));
            //获取分数（时间戳）
            long time = tuple.getScore().longValue();
            if (time < minTime) {
                minTime = time;
                os = 1;
            } else if (time == minTime) {
                os++;
            }
        }
        //根据id查询blog
        String idstr = StrUtil.join(",", ids)+ ")";
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id, " + idstr).list();
        blogs.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });


        //封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(os);
        return Result.ok(r);


    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }



    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            //用户未登录，无需查询否点赞
            return;
        }
        Long userId = user.getId();
        String key = "blog:like:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
}
