package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

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
        Boolean isLiked = stringRedisTemplate.opsForSet().isMember(key, user.getId().toString());
        if (BooleanUtil.isFalse(isLiked)) {
            //如果未点赞，则点赞
            //更新数据库
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
            stringRedisTemplate.opsForSet().add(key, user.getId().toString());
            }
        } else {
            //如果已经点赞，则取消点赞
            //更新数据库
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
            stringRedisTemplate.opsForSet().remove(key, user.getId().toString());
            }
        }



        return null;
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }



    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        String key = "blog:like:" + blog.getId();
        Boolean isLiked = stringRedisTemplate.opsForSet().isMember(key, user.getId().toString());
        blog.setIsLike(isLiked);
    }
}
