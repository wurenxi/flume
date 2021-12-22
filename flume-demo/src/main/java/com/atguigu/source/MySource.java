package com.atguigu.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/22 18:03
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    // 声明数据的前后缀
    private String prefix;
    private String suffix;
    private Long delay;

    @Override
    public void configure(Context context) {
        prefix = context.getString("pre","pre-");
        suffix = context.getString("suf");
        delay = context.getLong("delay",2000L);
    }

    @Override
    public Status process() throws EventDeliveryException {
        // 2.循环创建事件信息，传给channel
        try {
            for (int i = 0; i < 5; i++) {
                // 1.声明事件
                Event event = new SimpleEvent();
                Map<String,String> header = new HashMap<>();
                event.setHeaders(header);
                event.setBody((prefix+"atguigu:"+i+suffix).getBytes());
                getChannelProcessor().processEvent(event);
            }

            Thread.sleep(delay);
            return Status.READY;
        } catch (Exception exception) {
            exception.printStackTrace();
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
