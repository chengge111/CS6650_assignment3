import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Main {
  private static final String QUEUE_NAME = "skiersQueue";
  private static final String RABBITMQ_HOST = "172.31.25.112";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin123";

  private static final String DB_URL = "jdbc:mysql://localhost:3306/cs6650";
  private static final String DB_USER = "csuser";
  private static final String DB_PASSWORD = "cs6650pass";

  private static final int THREAD_COUNT = 100;
  private static final Gson gson = new Gson();

  public static void main(String[] args) {
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(RABBITMQ_HOST);
      factory.setUsername(USERNAME);
      factory.setPassword(PASSWORD);
      com.rabbitmq.client.Connection connection = factory.newConnection();

      ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
      for (int i = 0; i < THREAD_COUNT; i++) {
        executorService.submit(new ConsumerThread(connection));
      }

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          System.out.println("Shutting down Consumer...");
          executorService.shutdown();
          connection.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }));

    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
  }

  static class ConsumerThread implements Runnable {
    private final com.rabbitmq.client.Connection connection;
    private Channel channel;
    private java.sql.Connection dbConnection;

    public ConsumerThread(com.rabbitmq.client.Connection connection) {
      this.connection = connection;
      this.channel = createChannel();
      this.dbConnection = createDbConnection();
    }

    @Override
    public void run() {
      try {
        System.out.println(Thread.currentThread().getName() + " [*] Waiting for messages from RabbitMQ...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
          processMessage(message, dbConnection);
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
      } catch (IOException e) {
        System.out.println("Consumer encountered an error. Restarting...");
        reconnect();
      }
    }

    private Channel createChannel() {
      try {
        Channel ch = connection.createChannel();
        ch.queueDeclare(QUEUE_NAME, true, false, false, null);
        return ch;
      } catch (IOException e) {
        System.out.println("Failed to create RabbitMQ channel. Retrying...");
        reconnect();
        return null;
      }
    }

    private java.sql.Connection createDbConnection() {
      try {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to create DB connection.");
      }
    }

    private void reconnect() {
      while (true) {
        try {
          Thread.sleep(5000);
          this.channel = createChannel();
          if (this.channel != null) {
            System.out.println("Reconnected to RabbitMQ.");
            break;
          }
        } catch (InterruptedException ignored) {}
      }
    }
  }

  private static void processMessage(String message, java.sql.Connection conn) {
    try {
      JsonObject json = gson.fromJson(message, JsonObject.class);
      int skierID = json.get("skierID").getAsInt();
      int resortID = json.get("resortID").getAsInt();
      int liftID = json.get("liftID").getAsInt();
      int dayNum = json.get("dayNum").getAsInt();
      int time = json.get("time").getAsInt();
      int seasonID = json.get("seasonID").getAsInt();

      String sql = "INSERT INTO LiftRides (skierID, resortID, liftID, dayNum, time, seasonID) VALUES (?, ?, ?, ?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setInt(1, skierID);
        stmt.setInt(2, resortID);
        stmt.setInt(3, liftID);
        stmt.setInt(4, dayNum);
        stmt.setInt(5, time);
        stmt.setInt(6, seasonID);
        stmt.executeUpdate();
      }
      System.out.println("Inserted to DB: skierID " + skierID + ", liftID " + liftID);
    } catch (Exception e) {
      System.err.println("Failed to parse/insert message: " + message);
      e.printStackTrace();
    }
  }
}
