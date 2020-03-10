package flink;
/*
 *@description
 *@author:yanlinshan
 *@date: -
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class SocketTalkServer {

    public static void main(String[] args) {
        try {
            ServerSocket server = null;
            // 创建一个端口为9000监听客户端请求的serversocket
            try {
                server = new ServerSocket(39001);
                System.out.println("服务端启动成功：服务端端口号为9000");
            } catch (IOException e) {
                // 如果连接不上，打印出错信息
                System.out.println("can not listen to:"+e);
            }
            Socket serverSocket = null;
            try {
                // 使用accept()阻塞等待客户请求，有客户请求则产生一个Socket对象，并继续执行
                serverSocket = server.accept();
                // 有客户端连接
                System.out.println("有个客户端连接："+serverSocket.getInetAddress()+":"+serverSocket.getPort());
            } catch (IOException e) {
                // 客户端请求异常
                System.out.println(e);
            }
            String line;
            // 通过Socket对象得到输出流，构造printwriter对象
            PrintWriter serverPrintWriter = new PrintWriter(serverSocket.getOutputStream());
            // 通过控制台构造bufferedreader对象
            BufferedReader serverInput = new BufferedReader(new InputStreamReader(System.in));
            // 服务端控制台上输入的数据源字符串
            String serverLine = getRandomString();
            // 如果输入bye，停止循环
            while (!serverLine.equals("bye")){
                // 向客户端输出字符串
                serverPrintWriter.println(serverLine);
                // 刷新输出流
                serverPrintWriter.flush();
                // 在系统控制台上打印输入的内容；
                System.out.println("Server:"+serverLine);
                Thread.sleep(100);
                // 继续输入然后重新读取字符串

                serverLine = getRandomString();
            }
            serverPrintWriter.close();
            serverSocket.close();
            server.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String getRandomString(){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        int length = random.nextInt(13);
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}