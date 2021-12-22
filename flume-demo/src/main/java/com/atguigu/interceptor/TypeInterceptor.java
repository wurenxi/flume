package com.atguigu.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/21 15:45
 */
public class TypeInterceptor implements Interceptor {
    // 声明一个集合用于存放拦截器处理后的事件
    private List<Event> addHeaderEvents;

    // 初始化集合
    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }

    // 单个事件处理方法
    @Override
    public Event intercept(Event event) {
        // 1.获取header&body
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        // 2.根据body中是否包含"atguigu"添加不同的头信息
        if(body.contains("atguigu")){
            headers.put("type","atguigu");
        }else{
            headers.put("type","other");
        }

        // 返回数据
        return event;
    }

    // 批量事件处理方法
    @Override
    public List<Event> intercept(List<Event> events) {
        // 1.清空集合
        addHeaderEvents.clear();

        // 2.遍历
        for (Event event : events) {
            addHeaderEvents.add(intercept(event));
        }

        // 3.返回数据
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
