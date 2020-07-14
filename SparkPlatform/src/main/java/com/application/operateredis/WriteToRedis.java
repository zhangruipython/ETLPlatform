package com.application.operateredis;

import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.Objects.requireNonNull;

/**
 * @author 张睿
 * @create 2020-07-02 15:54
 **/
public class WriteToRedis implements Runnable{
    private final Jedis jedis;
    private final List<Map<String,Object>> mapList;
    private int startIndex;
    private final CountDownLatch countDownLatch;


    public WriteToRedis(Jedis jedis, List<Map<String, Object>> mapList,int startIndex,CountDownLatch countDownLatch) {
        requireNonNull(jedis,"redis client is null");
        requireNonNull(mapList,"map list is null");
        this.jedis = jedis;
        this.mapList = mapList;
        this.startIndex = startIndex;
        this.countDownLatch = countDownLatch;
    }

    /**
     * 通过pipeline机制分批次写入redis
     * @author 张睿
     * @date 2020-07-06
     * @return void
     */

    public void writeRedisPipe(){
        try {
            Pipeline pipeline = jedis.pipelined();
            for (Map map:mapList){
                pipeline.hset("user_profile:user_info:"+ startIndex,map);
                if(startIndex%2000==0){
                    pipeline.sync();
                    pipeline.clear();
                }
                startIndex++;
            }
            pipeline.sync();
        } finally {
            System.out.println("线程执行结束");
            jedis.disconnect();
            jedis.close();
            countDownLatch.countDown();
        }
    }
    @Override
    public void run() {
        writeRedisPipe();
    }
}

