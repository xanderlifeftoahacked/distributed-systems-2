package distr.common;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class NetworkClient {
    private NetworkClient() {
    }

    public static ObjectNode sendRequest(String host, int port, ObjectNode request, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                writer.write(JsonUtil.toJson(request));
                writer.write("\n");
                writer.flush();
                String line = reader.readLine();
                if (line == null) {
                    throw new IOException("No response");
                }
                return JsonUtil.parseObject(line);
            }
        }
    }

    public static boolean sendOneWay(String host, int port, ObjectNode request, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
                writer.write(JsonUtil.toJson(request));
                writer.write("\n");
                writer.flush();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }
}

