package com.application.operateredis;

/**
 * @author 张睿
 * @create 2020-07-03 16:02
 **/

import com.alibaba.fastjson.JSON;
import com.application.util.ValueDecode;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import redis.clients.jedis.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class RocksDbToRedis {
    private final JedisPool jedisPool;
    private final Options options = new Options().setCreateIfMissing(true);
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    static {
        RocksDB.loadLibrary();
    }
    public RocksDbToRedis(String redisHost, Integer redisPort) {
        requireNonNull(redisHost,"redis host is null");
        requireNonNull(redisPort,"redis port is null");
        HostAndPort hostAndPort = new HostAndPort(redisHost,redisPort);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setMaxTotal(20);
        this.jedisPool = new JedisPool(jedisPoolConfig,redisHost,redisPort);
    }
    /**
    *  将rocksdb中数据转为 Map对象
    * @author 张睿
    * @date 2020-07-03
    * @param dBPath
    * @return java.util.List<java.util.Map>
    */

    public List<Map<String,Object>> readRocksDb(String dBPath) throws RocksDBException {
        try(RocksDB rocksDB = RocksDB.openReadOnly(options,dBPath);
            RocksIterator iterator=rocksDB.newIterator()){
            List<byte[]> valueByteList = new LinkedList<>();
            ValueDecode valueDecode = new ValueDecode();
            // TODO: 对于千万级别数据，不能执行此操作，会导致Heap内存溢出
            for (iterator.seekToFirst();iterator.isValid();iterator.next()){
                valueByteList.add(iterator.value());
            }
            // 执行转换、过滤操作
            List<Map<String,Object>> valueMapList = valueByteList.parallelStream().map(s->new String(valueDecode.decodeValue(s),CHARSET))
                    .filter(s->s.startsWith("{")).map(s-> (Map<String,Object>)JSON.parseObject(s).entrySet().stream().filter(map-> map.getValue() instanceof String)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .collect(Collectors.toList());
            return valueMapList;
        }
    }

    public void multiExec(String dbPath) throws RocksDBException, InterruptedException {
        List<Map<String,Object>> mapList = readRocksDb(dbPath);
        // 一个线程处理2000条数据
        int singleCount = 20000;
        int allTaskCount = mapList.size();
        // TODO：根据任务分配确定开启线程数量，不一定效果最好。
        int threadCount = (allTaskCount/singleCount)+1;
        // 定义闭锁，线程计数器
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        List<Map<String,Object>> tasks;
        System.out.println("主线程开始执行");
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            for(int i=0;i<threadCount;i++){
                // 分配每个线程计算的数据
                tasks = i+1==threadCount?mapList.subList(i*singleCount,allTaskCount):mapList.subList(i*singleCount,(i+1)*singleCount);
                executorService.submit(new WriteToRedis(jedisPool.getResource(),tasks,i*singleCount,countDownLatch));
            }
            System.out.println("等待线程执行结束");
        } finally {
            countDownLatch.await();
            System.out.println("主线程恢复");
            executorService.shutdown();
            jedisPool.close();
        }
    }

    public static void main(String[] args) throws RocksDBException, InterruptedException {
        // 耗时 29107 添加多线程耗时25337 调整pipeline一次性执行命令总数后为 9798
        long time01 = System.currentTimeMillis();
        RocksDbToRedis rocksDbToRedis = new RocksDbToRedis("192.168.1.54",6379);
        rocksDbToRedis.multiExec("/var/rongzer/rbcdata/peer0/ledgersData/stateRocksdb");
        System.out.println("write to redis use time");
        System.out.println(System.currentTimeMillis()-time01);
    }
}

