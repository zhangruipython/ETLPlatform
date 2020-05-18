package com.application.stream;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author 张睿
 * @create 2020-05-18 17:25
 * 便于在window环境中测试 Spark Structured Streaming，使用java构建socket服务端
 **/
public class StreamSocketServer {

    public void doServer() throws IOException {
        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("服务端启动");
        Socket socket = serverSocket.accept();
        System.out.println("客户端已建立连接");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        OutputStream outputStream = socket.getOutputStream();
        PrintWriter printWriter = new PrintWriter(outputStream,true);
        System.out.println("enter lines of text");
        String str;
        do{
            str = bufferedReader.readLine();
            printWriter.println(str);
            System.out.println(str);
        }while (!str.equals("end"));
    }

    public static void main(String[] args) throws IOException {
        StreamSocketServer streamSocketServer = new StreamSocketServer();
        streamSocketServer.doServer();
    }
}

