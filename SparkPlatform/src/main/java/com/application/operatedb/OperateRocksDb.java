package com.application.operatedb;

/**
 * @author 张睿
 * @create 2020-04-23 15:45
 * load LevelDB中数据至spark
 **/

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperateRocksDb {
    static {
        RocksDB.loadLibrary();
    }
    public static final String DB_PATH = "/data/rocksdb";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    Options options = new Options().setCreateIfMissing(true);

    public void putData() throws RocksDBException{
        try (RocksDB rocksDB = RocksDB.open(options,DB_PATH)){
            rocksDB.put("she".getBytes(CHARSET),"world02,world02".getBytes(CHARSET));
        }
    }
    public void getData(String keyName) throws  RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(options,DB_PATH)){
            byte[] key = keyName.getBytes(CHARSET);
            List<byte[]> list = new ArrayList<byte[]>();
            list.add("key01".getBytes());
            list.add("key02".getBytes());
            rocksDB.multiGetAsList(list);

            String valueName = new String(rocksDB.get(key),CHARSET);
            System.out.println(valueName);
        }
    }

    /**rocksdb 遍历从第一个元素开始遍历所有元素
     * initialCapacity = (需要存储的元素个数 / 负载因子) + 1。注意负载因子（即loader factor）默认为0.75，
     * 如果暂时无法确定初始值大小，请设置为16（即默认值）*/
    public Map<String,String> iteratorFromFirst() throws RocksDBException{
        Map<String,String> kvMap = new HashMap<String, String>(16);
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            boolean seekToFirstPassed = false;
            for (iterator.seekToFirst();iterator.isValid();iterator.next()){
                iterator.status();
                kvMap.put(new String(iterator.key(),CHARSET),new String(iterator.value(),CHARSET));
                assert (iterator.key()!=null);
                assert (iterator.value()!=null);
                seekToFirstPassed = true;
            }
            if (seekToFirstPassed){
                System.out.println("迭代器 seekToFirst通过");
            }
        }
        return kvMap;
    }
    /**rocksdb 遍历从最后一个元素开始遍历所有元素*/
    public Map<String,String> iteratorFromLast() throws RocksDBException{
        Map<String,String> kvMap = new HashMap<String, String>(16);
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            boolean seekToLastPassed = false;
            for(iterator.seekToLast();iterator.isValid();iterator.prev()){
                iterator.status();
                kvMap.put(new String(iterator.key(),CHARSET),new String(iterator.value(),CHARSET));
                // assert condition :condition为false抛出异常
                assert (iterator.key()!=null||iterator.value()!=null);
                seekToLastPassed = true;
            }
            if (seekToLastPassed){
                System.out.println("迭代器 seekToLast通过");
            }
        }
        return kvMap;
    }
    /**rocksdb 模糊匹配key值，查询出符合条件的key value值
     * seek(option) 遍历出所有的数据只要key值中包含option
     * option :he ==>key: she,hello*/
    public void prefixIterKeys() throws RocksDBException{
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            byte[] seekKey = "he".getBytes(CHARSET);
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("s")){
                    System.out.println(new String(iterator.value(),CHARSET));
                }
            }
        }
    }


    public static void main(String[] args) throws RocksDBException {
        OperateRocksDb operateRocksDb = new OperateRocksDb();
        operateRocksDb.putData();
        operateRocksDb.prefixIterKeys();
//        Map<String,String> params = operateRocksDb.iteratorFromFirst();
//        System.out.println(params.toString());

    }
}

