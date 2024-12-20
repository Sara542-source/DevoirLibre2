import com.google.gson.*;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

class OrderProcessor {

    private static final String INPUT_FILE = "src/main/resources/input.json";
    private static final String OUTPUT_FILE = "src/main/resources/output.json";
    private static final String ERROR_FILE = "src/main/resources/error.json";

    public static void main(String[] args) {
        try {
            parseJsonThread();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void parseJsonThread() {
        new Thread(() -> {
            while (true) {
                try {
                    processOrders();
                    Thread.sleep(3600000); // Attendre 1 heure
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void processOrders() throws Exception {
        // Lire le contenu du fichier input.json
        String inputData = new String(Files.readAllBytes(Paths.get(INPUT_FILE)));
        Gson gson = new Gson();
        List<Order> orders = Arrays.asList(gson.fromJson(inputData, Order[].class));

        List<Order> validOrders = new ArrayList<>();
        List<Order> invalidOrders = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/devoir2", "root", "SaRaslim123?")) {
            for (Order order : orders) {
                if (isCustomerExists(connection, order.getCustomerId())) {
                    insertOrder(connection, order);
                    validOrders.add(order);
                } else {
                    invalidOrders.add(order);
                }
            }
        }

        // Écrire les commandes validées dans output.json
        writeToJsonFile(OUTPUT_FILE, validOrders);

        // Écrire les commandes invalides dans error.json
        writeToJsonFile(ERROR_FILE, invalidOrders);

        // Vider le fichier input.json
        Files.write(Paths.get(INPUT_FILE), new byte[0]);
    }

    private static boolean isCustomerExists(Connection connection, int customerId) throws SQLException {
        String query = "SELECT COUNT(*) FROM customer WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    private static void insertOrder(Connection connection, Order order) throws SQLException {
        String query = "INSERT INTO `order` (id, date, amount, customer_id) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, order.getId());
            ps.setString(2, order.getDate());
            ps.setDouble(3, order.getAmount());
            ps.setInt(4, order.getCustomerId());
            ps.executeUpdate();
        }
    }

    private static void writeToJsonFile(String filePath, List<Order> orders) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(orders);
        Files.write(Paths.get(filePath), json.getBytes());
    }


}
