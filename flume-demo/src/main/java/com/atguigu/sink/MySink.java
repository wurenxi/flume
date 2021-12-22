package com.atguigu.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/22 19:04
 */
public class MySink extends AbstractSink implements Configurable {
    // 声明数据的前后缀
    private String prefix;
    private String suffix;

    // 创建logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    @Override
    public void configure(Context context) {
        prefix = context.getString("pre","pre-");
        suffix = context.getString("suf");
    }

    @Override
    public Status process() throws EventDeliveryException {
        // 1.获取Channel并开启事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        transaction.begin();

        // 2.从channel中抓取数据打印到控制台
        try{
            // 2.1 抓取数据
            Event event;
            do {
                event = channel.take();
            } while (event == null);

            // 2.2处理数据
            logger.info(prefix+new String(event.getBody())+suffix);

            // 2.3提交事务
            transaction.commit();

            return Status.READY;
        }catch (Exception e){
            // 回滚
            transaction.rollback();
            return Status.BACKOFF;
        }finally {
            transaction.close();
        }
    }
}
