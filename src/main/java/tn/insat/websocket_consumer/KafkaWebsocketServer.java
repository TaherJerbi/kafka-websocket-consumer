package tn.insat.websocket_consumer;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

public class KafkaWebsocketServer extends WebSocketServer {

    private KafkaConsumer<String, String> consumer;

    public KafkaWebsocketServer(InetSocketAddress address) {
        super(address);
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Arrays.asList("reddit-comments-sentiments"));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        // Nothing to do here
        new Thread(() -> {
            while (true) {
                try {
                    ConsumerRecords<String, String> records =  consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("--- Got Record: "+record.value());
                        conn.send(record.value());
                        System.out.println("---- Sent");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        // Nothing to do here
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // Nothing to do here
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        ex.printStackTrace();
    }


    public static void main(String[] args) {
        KafkaWebsocketServer server = new KafkaWebsocketServer(new InetSocketAddress("localhost", 8080));
        server.start();
    }
}
