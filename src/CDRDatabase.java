import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CDRDatabase {
    private Connection connection;

    // CONSTRUCTOR CORREGIDO - acepta parámetros
    public CDRDatabase(String url, String username, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, username, password);
        createTables();
    }

    private void createTables() throws SQLException {
        // ... (código de creación de tablas)
    }

    // ✅ MeTODO INSERT CDR - agregar este metodo
    public void insertCDR(CDR cdr) throws SQLException {
        String sql = "INSERT INTO cdr_records (account_number, calling_number, called_number, timestamp, duration_minutes, cost, call_type) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, cdr.getAccountNumber());
            pstmt.setString(2, cdr.getCallingNumber());
            pstmt.setString(3, cdr.getCalledNumber());
            pstmt.setString(4, cdr.getTimestamp());
            pstmt.setInt(5, cdr.getDurationMinutes());
            pstmt.setDouble(6, cdr.getCost());
            pstmt.setString(7, cdr.getCallType());
            pstmt.executeUpdate();
        }
    }

    //metodo update summary
    public void updateAccountSummary(String accountNumber, int duration, double cost, String callType) throws SQLException {
        String sql = "INSERT INTO account_summary (account_number, total_minutes, total_cost, international_calls, national_calls, local_calls) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE total_minutes = total_minutes + ?, total_cost = total_cost + ?, international_calls = international_calls + ?, national_calls = national_calls + ?, local_calls = local_calls + ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, accountNumber);
            pstmt.setInt(2, duration);
            pstmt.setDouble(3, cost);
            pstmt.setInt(4, "internacional".equals(callType) ? 1 : 0);
            pstmt.setInt(5, "nacional".equals(callType) ? 1 : 0);
            pstmt.setInt(6, "local".equals(callType) ? 1 : 0);
            pstmt.setInt(7, duration);
            pstmt.setDouble(8, cost);
            pstmt.setInt(9, "internacional".equals(callType) ? 1 : 0);
            pstmt.setInt(10, "nacional".equals(callType) ? 1 : 0);
            pstmt.setInt(11, "local".equals(callType) ? 1 : 0);
            pstmt.executeUpdate();
        }
    }

    //metodo get summary
    public List<String> getAccountSummary() throws SQLException {
        List<String> results = new ArrayList<>();
        String sql = "SELECT account_number, total_minutes, total_cost, international_calls, national_calls, local_calls FROM account_summary ORDER BY total_cost DESC";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String result = String.format("Cuenta: %s | Minutos: %d | Costo: Q%.2f | Int: %d | Nac: %d | Loc: %d",
                        rs.getString("account_number"), rs.getInt("total_minutes"), rs.getDouble("total_cost"),
                        rs.getInt("international_calls"), rs.getInt("national_calls"), rs.getInt("local_calls"));
                results.add(result);
            }
        }
        return results;
    }

    //metodo get connection
    public Connection getConnection() throws SQLException {
        return connection;
    }

    public void close() throws SQLException {
        if (connection != null) connection.close();
    }
}