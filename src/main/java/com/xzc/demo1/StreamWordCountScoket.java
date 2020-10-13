package com.xzc.demo1;

import org.apache.commons.lang3.RandomUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * 流处理 socket
 *
 * @author xzc
 */
public class StreamWordCountScoket {
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(7878);

        Socket socket = serverSocket.accept();
        OutputStream outputStream = socket.getOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        String[] messageArray = new String[]{"hello word", "hello flink", "hello java", "how are you", "flink who are you"};
        while (true) {
            int index = RandomUtils.nextInt(0,4);
            dataOutputStream.writeUTF(messageArray[index]);
            dataOutputStream.flush();
            TimeUnit.SECONDS.sleep(2L);
        }
    }
}
