package com.application.operatedb;

import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.WriteBatchImpl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * @author 张睿
 * @create 2020-04-22 15:21
 **/
public class OperateLeveldb {
    private static final String DB_PATH = "/home/hadoop/Documents/leveldb/testdb";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final DBFactory factory = new Iq80DBFactory();
    private final Options options = new Options().createIfMissing(true);
    /**是否压缩
     * CompressionType.NONE 不使用压缩
     * CompressionType.SNAPPY 使用Google的Snappy压缩，速度快，压缩文件大
     * */
    private final Options options_disable_compress = new Options().compressionType(CompressionType.NONE);
    /**配置缓存空间
     * 100*1048576 = 100M
     * */
    private final Options options_cache = new Options().cacheSize(100*1048576);
    public void putTest(){
        DBFactory factory = new Iq80DBFactory();
        Options options = new Options();
        File file = new File(DB_PATH);
        DB db = null;
        try {
            db = factory.open(file,options);
            // 写入数据库
            byte[] keyByte01 = "key-01".getBytes(CHARSET);
            byte[] keyByte02 = "key-02".getBytes(CHARSET);
            db.put(keyByte01,"value-01".getBytes(CHARSET));
            db.put(keyByte02,"value-02".getBytes(CHARSET));

            // 在数据库中查询
            String value01 = new String(db.get(keyByte01),CHARSET);
            System.out.println(value01);
            System.out.println(new String(db.get(keyByte02),CHARSET));
        } catch (IOException e){
            e.printStackTrace();
        }finally {
            if (db!=null){
                try {
                    db.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
    public void putDemo() throws IOException {
        // java7 语句特性，try-with-resource try包裹内容自动关闭
        try (DB db = factory.open(new File(DB_PATH), options)) {
            // 添加key value数据
            db.put(bytes("hello01"), bytes("world"));
            db.put(bytes("hello02"), bytes("jack"));
            db.put(bytes("hello03"), bytes("name"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void getDemo() throws IOException {
        try (DB db = factory.open(new File(DB_PATH), options)) {
            // 根据key查询value
            String value = asString(db.get(bytes("hello")));
            System.out.println(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void iterateDemo() throws IOException{
        // 迭代元素
        long startTime = System.currentTimeMillis();
        try (DB db = factory.open(new File(DB_PATH),options)){
            DBIterator iterator = db.iterator();
            int num = 0;
            for (iterator.seekToFirst();iterator.hasNext();iterator.next()){
                String key =asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                num = num+1;
            }
            System.out.println(num);
            System.out.println(String.format("耗时%s",System.currentTimeMillis()-startTime));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void customComparator(){
        // 自定义比较器
        DBComparator comparator = new DBComparator() {
            @Override
            public String name() {
                return null;
            }

            @Override
            public byte[] findShortestSeparator(byte[] start, byte[] limit) {
                return new byte[0];
            }

            @Override
            public byte[] findShortSuccessor(byte[] key) {
                return new byte[0];
            }

            @Override
            public int compare(byte[] o1, byte[] o2) {
                // 如果相等返回0，如果指定数小于参数返回-1，如果指定数大于参数返回1
                return new String(o1).compareTo(new String(o2));
            }
        };
    }
    /**使用GetApproximateSizes()获取一个或多个划定的键范围被保存在文件中所占空间大小
     * [a,k)和[k,z)范围内所有key所占空间大小*/
    public void getSize() throws IOException{
        try (DB db = factory.open(new File(DB_PATH),options)){
            long[] sizes = db.getApproximateSizes(new Range(bytes("a"),bytes("k")),
                    new Range(bytes("k"),bytes("z")));
            String dbStats = db.getProperty("leveldb.stats");
            System.out.println(Arrays.toString(sizes));
            System.out.println(dbStats);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**批量写操作*/
    public void writeAction() throws IOException{

        try (DB db = factory.open(new File(DB_PATH),options)){
            WriteBatchImpl writeBatch = new WriteBatchImpl().put(bytes("name"),bytes("jack"));

            db.write(writeBatch);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**获取信息日志消息*/
    public void getMessageLog() throws IOException{
        Logger logger = new Logger() {
            @Override
            public void log(String message) {
                System.out.println(message);
            }
        };
        Options options01 = new Options();
        options01.logger(logger);
        try (DB db = factory.open(new File(DB_PATH),options01)){
            db.put(bytes("key001"),bytes("value001"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**去除一个数据库*/
    public void destroyDatabase() throws IOException {
        factory.destroy(new File("example"), options);
    }

    public static void main(String[] args) throws IOException {
        OperateLeveldb demo = new OperateLeveldb();
        demo.iterateDemo();
//        demo.writeAction();
//        demo.getMessageLog();
    }
}