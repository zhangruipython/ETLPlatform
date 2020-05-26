package com.application.operatedb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * @author 张睿
 * @create 2020-05-19 10:27
 * 查看rocksdb中数据结构
 **/
public class CheckStructure {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    Options options = new Options().setCreateIfMissing(true);

    static {
        RocksDB.loadLibrary();
    }
    public void check(String dbPath,String logPath) throws FileNotFoundException, RocksDBException {
        File file = new File(logPath);
        PrintStream printStream = new PrintStream(new FileOutputStream(file,true));
        try (RocksIterator iterator = RocksDB.open(options,dbPath).newIterator()){
            for (iterator.seekToFirst();iterator.isValid();iterator.next()){
                printStream.println(new String(iterator.key(),CHARSET)+" : "+new String(iterator.value(),CHARSET));
            }
        }
    }
    public void demo(String logPath) throws FileNotFoundException {
        File file = new File(logPath);
        PrintStream printStream = new PrintStream(file);
        List<String> stringList = Arrays.asList("hello","zhangrui","spark");
        for(String i:stringList){
            printStream.println(i);
        }
    }

    public static void main(String[] args) throws FileNotFoundException, RocksDBException {
        String dbPath = args[0];
        String logPath = args[1];
        CheckStructure checkStructure = new CheckStructure();
        checkStructure.check(dbPath,logPath);

    }
}

