import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CDRDatabase {
    public class CDRDatabase {
        private Connection connection; //conexion a la BD

        //se establece conexion con la BD
        public CDRDatabase(String url, String username, String password) throws SQLException {
            this.connection = DriverManager.getConnection(url, username, password);
            createTables();
        }

        //crea tablas necesarias en caso no existieran en MySQL
        private void createTables() throws SQLException {
            String createCDRTable = """
            
                    CREATE TABLE IF NOT EXISTS cdr_records (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                account_number VARCHAR(20),
                calling_number VARCHAR(20),
                called_number VARCHAR(20),
                timestamp VARCHAR(50),
                duration_minutes INTEGER,
                cost DECIMAL(10,2),
                call_type VARCHAR(20),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
            String createSummaryTable =
                    """
            CREATE TABLE IF NOT EXISTS
                    account_summary (
                account_number
                    VARCHAR(20) PR
                                  total_minutes I
                               total_cost DECIMAL(10,2),
                                international_calls INTEGER DEF
                               national_calls INTEGER 
                                  local_calls INTEGER DEFAULT 0,
                                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createCDRTable);
                stmt.execute(createSummaryTable);
            }
        }

        //metodos de insercion y actualizacion
        public void insertCDR(CDR cdr) throws SQLException {
            String callType = cdr.
                    getCallType();

            String sql = """
            INSERT INTO cdr_records 
            (account_number, calling_number,
                    called_number, timestamp,
                    duration_minutes, cost, call_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, cdr.getAccountNumber());
                pstmt.setString(2, cdr.getCallingNumber());
                pstmt.setString(3, cdr.getCalledNumber());
                pstmt.setString(4, cdr.getTimestamp());
                pstmt.setInt(5, cdr.getDurationMinutes());
                pstmt.setDouble(6, cdr.getCost());
                pstmt.

        setString(7, callType);
                pstmt.executeUpdate();
            }
        }
        //actualiza el resumen de la cuenta con los datos de la llamada
        public void updateAccountSummary(String accountNumber, int
                    duration, double cost, String callType) throws SQLException {
                    String updateSql = """
            INSERT INTO account_summary (account_number, total_minutes,
                    total_cost,           
                    international_calls, national_calls, local_calls)
            VALUES (?, ?, ?, ?,
                          ON DUPLICATE KEY UPDATE
            total_minutes =
                    total_minutes + VALUES(total_minutes),
            total_cost =
                    total_cost + VALUES(total_cost),
            international_calls =
                    international_calls + VALUES(international_calls)
                    ,
            national_calls =
                    national_calls + VALUES(national_calls),
            local_calls = local_calls + VALUES(local_calls),
            last_updated = CURRENT_TIMESTAMP
            """;

            try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
                pstmt.setString(1, accountNumber);
                pstmt.setInt(2, duration);
                pstmt.setDouble(3, cost);

                pstmt.setInt(4, "internacional".equals(callType) ? 1 : 0);
                pstmt.setInt(5, "nacional".equals(callType) ? 1 : 0);
                pstmt.setInt(6, "local".equals(callType) ? 1 : 0);

                pstmt.executeUpdate();
            }
        }

        //metodos de corte
        public List<String> getAccountSummary() throws
                    SQLException {
            List<String> results = new
                    ArrayList<>();
                    String sql = """
            SELECT
                    account_number, total_minutes, total_cost, 
                   international_calls, national_calls, local_calls
            FROM account_summary 
            ORDER BY total_cost DESC
            """;

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                while (rs.next()) {
                    String result = String.format("Cuenta: %s | Minutos: %d | Costo: Q%.2f | Int: %d | Nac: %d | Loc: %d",
                            rs.getString("account_number"),
                            rs.getInt("total_minutes"),
                            rs.getDouble("total_cost"),
                            rs.getInt("international_calls"),
                            rs.getInt("national_calls"),
                            rs.getInt("local_calls"));
                    results.add(result);
                }
            }

            if (results.isEmpty()) {
                results.add("No hay datos en la base de datos.");
                results.add("Ejecuta 'Iniciar Procesamiento' primero con un archivo CSV.");
            }

            return results;
        }

        //metodos auxiliares
        public boolean hasData() throws SQLException {
            String sql = "SELECT COUNT(*) as count FROM account_summary";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt("count") > 0;
                }
            }
            return false;
        }

        public Connection getConnection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                throw new SQLException("Conexión no disponible");
            }
            return connection;
        }

        //recrea tablas testing
        public void recreateTables() throws SQLException {
            String dropCDRTable = "DROP TABLE IF EXISTS cdr_records";
            String dropSummaryTable = "DROP TABLE IF EXISTS account_summary";

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropCDRTable);
                stmt.execute(dropSummaryTable);
            }

            createTables();
        }

        public void close() throws SQLException {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }
}