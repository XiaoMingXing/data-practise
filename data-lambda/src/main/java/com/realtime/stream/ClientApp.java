package com.realtime.stream;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientApp {

    public static void main(String... args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(9087);
        Socket clientSocket = serverSocket.accept();
        OutputStream outputStream = clientSocket.getOutputStream();

        while (true) {
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String data = reader.readLine();
            System.out.println("Data received and now writing to socket: " + data);
            printWriter.println(data);
        }
    }
}
