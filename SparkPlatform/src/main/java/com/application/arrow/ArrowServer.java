package com.application.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * @author 张睿
 * @create 2020-05-29 9:40
 **/
public class ArrowServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("服务端启动");
        Socket clientSocket = serverSocket.accept();
        System.out.println("客户端建立连接");
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        VarCharVector vector = new VarCharVector("demo",allocator);
        vector.allocateNewSafe();
        // vector 填充数据
        vector.setSafe(0,"arrow".getBytes(StandardCharsets.UTF_8));
        vector.setSafe(1,"apache".getBytes(StandardCharsets.UTF_8));
        vector.setSafe(2,"hadoop".getBytes(StandardCharsets.UTF_8));
        vector.setValueCount(3);
        // 创建VectorSchemaRoot
        List<Field> fields = Collections.singletonList(vector.getField());
        List<FieldVector> fieldVectors = Collections.singletonList(vector);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields,fieldVectors);
        System.out.println(vectorSchemaRoot.getVector(0));
        System.out.println(vectorSchemaRoot.getVector("demo"));
        System.out.println(vectorSchemaRoot.getSchema());
        System.out.println(vectorSchemaRoot.getRowCount());
        // 数据写入流中
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream outputStream = clientSocket.getOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot,/*DictionaryProvider=*/null,outputStream);
        writer.start();
        int rowCount = vectorSchemaRoot.getRowCount();
        if(rowCount==0){
            writer.end();
        }else {
            System.out.println("写入");
            writer.writeBatch();
            Thread.sleep(10000);
        }
    }
}

