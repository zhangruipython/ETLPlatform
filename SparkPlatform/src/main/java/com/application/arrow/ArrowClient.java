package com.application.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * @author 张睿
 * @create 2020-05-29 10:13
 **/
public class ArrowClient {
    public static void main(String[] args) throws IOException {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Socket socket = new Socket("localhost",9999);
        InputStream inputStream = socket.getInputStream();
        try(ArrowStreamReader reader = new ArrowStreamReader(inputStream,allocator)){
            reader.loadNextBatch();
            VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
            System.out.println(readRoot.getVector(0));
            VectorUnloader unLoader = new VectorUnloader(readRoot);
            ArrowRecordBatch recordBatch = unLoader.getRecordBatch();
            System.out.println(recordBatch.getLength());
            System.out.println(recordBatch.getNodes());
        }
}
}

