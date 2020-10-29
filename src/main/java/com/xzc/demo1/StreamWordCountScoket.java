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
        ServerSocket serverSocket = new ServerSocket(7979);

        Socket socket = serverSocket.accept();
        OutputStream outputStream = socket.getOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        String[] messageArray = new String[]{
                "station1,18688822219,18684812319,10,1595158485855",
                "station2,19688822219,28684812319,20,1595158485856",
                "station3,28688822219,38684812319,30,1595158485857",
                "station4,13688822219,48684812319,40,1595158485858",
                "station5,15688822219,58684812319,50,1595158485859"};
//        String[] messageArray = new String[]{"hello word", "hello flink", "hello java", "how are you", "flink who are you"};
        while (true) {
            int index = RandomUtils.nextInt(0,4);
            try {
                dataOutputStream.writeUTF(messageArray[index]);
            } catch (Exception e) {

            }
            dataOutputStream.flush();
            TimeUnit.SECONDS.sleep(2L);
        }
    }
}
