import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

//interfaz grafica principal del sistema de procesamiento CDR
public class CDRGUI extends JFrame {
    //componentes de la interfaz
    private CDRProcessor processor;
    private JTextArea producerStatusArea;
    private JTextArea consumerStatusArea;
    private JTextArea resultsArea;
    private JButton startButton;
    private JButton stopButton;
    private Timer statusTimer;
    private CDRDatabase database;

    // Constructor principal
    public CDRGUI() {
        try {
            // Conexión a base de datos
            database = new CDRDatabase(
                    "jdbc:mysql://localhost:3306/cdr_db",
                    "cdr_user",
                    "Seguridad25+"
            );
            this.processor = new CDRProcessor(database);
            initializeGUI();
            System.out.println("GUI inicializada");
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(this, "Error de conexión BD: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Error inicial: " + e.getMessage());
            System.exit(1);
        }
    }

    //inicializacion de componentes graficos
    private void initializeGUI() {
        setTitle("Sistema de Procesamiento CDR");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLayout(new BorderLayout());

        //panel de botones de control
        JPanel controlPanel = new JPanel();
        startButton = new JButton("Iniciar Procesamiento");
        stopButton = new JButton("Detener Procesamiento");
        JButton resultsButton = new JButton("Mostrar Resultados");
        JButton debugButton = new JButton("Debug BD");

        stopButton.setEnabled(false); //inicialmente deshabilitado

        controlPanel.add(startButton);
        controlPanel.add(stopButton);
        controlPanel.add(resultsButton);
        controlPanel.add(debugButton);
        add(controlPanel, BorderLayout.NORTH);

        //panel de estado (productores y consumidores)
        JPanel statusPanel = new JPanel(new GridLayout(1, 2));
        producerStatusArea = new JTextArea();
        consumerStatusArea = new JTextArea();

        producerStatusArea.setEditable(false);
        consumerStatusArea.setEditable(false);
        producerStatusArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        consumerStatusArea.setFont(new Font("Monospaced", Font.PLAIN, 12));

        producerStatusArea.setBackground(new Color(240, 240, 240));
        consumerStatusArea.setBackground(new Color(240, 240, 240));

        statusPanel.add(new JScrollPane(producerStatusArea));
        statusPanel.add(new JScrollPane(consumerStatusArea));
        add(statusPanel, BorderLayout.CENTER);

        //panel de resultados
        resultsArea = new JTextArea();
        resultsArea.setEditable(false);
        resultsArea.setBackground(new Color(255, 255, 220));
        resultsArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        add(new JScrollPane(resultsArea), BorderLayout.SOUTH);

        //manejadores de eventos
        startButton.addActionListener(this::startProcessing);
        stopButton.addActionListener(this::stopProcessing);
        resultsButton.addActionListener(this::showResults);
        debugButton.addActionListener(this::debugDatabase);

        //timer para actualizar estado cada segundo
        statusTimer = new Timer(1000, e -> updateStatus());
        statusTimer.start();

        setVisible(true);
    }

    //iniciar procesamiento de archivo CSV
    private void startProcessing(ActionEvent e) {
        if (processor == null) return;

        JFileChooser fileChooser = new JFileChooser();
        if (fileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            String filePath = fileChooser.getSelectedFile().getAbsolutePath();
            processor.startProcessing(filePath, 3, 2); // 3 productores, 2 consumidores
            startButton.setEnabled(false);
            stopButton.setEnabled(true);

            resultsArea.setText("");
            resultsArea.append("Procesamiento iniciado...\n");
            resultsArea.append("Archivo: " + filePath + "\n");
        }
    }

    //detener procesamiento
    private void stopProcessing(ActionEvent e) {
        if (processor != null) {
            processor.stopProcessing();
        }
        startButton.setEnabled(true);
        stopButton.setEnabled(false);
        resultsArea.append("Procesamiento detenido");
    }

    //mostrar resultados del procesamiento
    private void showResults(ActionEvent e) {
        if (processor == null) return;

        try {
            List<String> results = processor.getResults();
            resultsArea.setText("");

            //encabezado de resultados
            resultsArea.append("=== RESULTADOS DE CUENTAS ===\n\n");
            resultsArea.append("CUENTA     | MINUTOS | COSTO    | INT | NAC | LOC\n");
            resultsArea.append("-------------------------------------------------\n");

            if (results.isEmpty()) {
                resultsArea.append("No se encontraron resultados.\n");
            } else {
                int totalInt = 0, totalNac = 0, totalLoc = 0;
                int totalMinutos = 0;
                double totalCosto = 0;

                //procesar cada resultado
                for (String result : results) {
                    resultsArea.append(result + "\n");

                    if (result.contains("Int:") && result.contains("Nac:") && result.contains("Loc:")) {
                        try {
                            String[] parts = result.split("\\|");
                            if (parts.length >= 6) {
                                totalMinutos += Integer.parseInt(parts[1].replace("Minutos:", "").trim());
                                totalCosto += Double.parseDouble(parts[2].replace("Costo: Q", "").trim());
                                totalInt += Integer.parseInt(parts[3].replace("Int:", "").trim());
                                totalNac += Integer.parseInt(parts[4].replace("Nac:", "").trim());
                                totalLoc += Integer.parseInt(parts[5].replace("Loc:", "").trim());
                            }
                        } catch (NumberFormatException ex) {
                            //ignorar errores de parsing
                        }
                    }
                }

                //mostrar resumen general
                resultsArea.append("\n=== RESUMEN GENERAL ===\n");
                resultsArea.append("Total cuentas: " + results.size() + "\n");
                resultsArea.append("Total minutos: " + totalMinutos + "\n");
                resultsArea.append("Total costo: Q" + String.format("%.2f", totalCosto) + "\n");

                //mostrar distribución por tipo de llamada
                resultsArea.append("\n=== DISTRIBUCIÓN POR TIPO ===\n");
                resultsArea.append("Internacionales: " + totalInt + "\n");
                resultsArea.append("Nacionales: " + totalNac + "\n");
                resultsArea.append("Locales: " + totalLoc + "\n");

                //calcular porcentajes
                int totalLlamadas = totalInt + totalNac + totalLoc;
                if (totalLlamadas > 0) {
                    resultsArea.append("\n=== PORCENTAJES ===\n");
                    resultsArea.append("Internacionales: " + String.format("%.1f", (totalInt * 100.0 / totalLlamadas)) + "%\n");
                    resultsArea.append("Nacionales: " + String.format("%.1f", (totalNac * 100.0 / totalLlamadas)) + "%\n");
                    resultsArea.append("Locales: " + String.format("%.1f", (totalLoc * 100.0 / totalLlamadas)) + "%\n");
                }
            }

            resultsArea.setCaretPosition(0);

        } catch (Exception ex) {
            resultsArea.setText("Error al obtener resultados:\n" + ex.getMessage());
        }
    }

    //debug de base de datos
    private void debugDatabase(ActionEvent e) {
        try {
            java.sql.Statement stmt = database.getConnection().createStatement();

            //verificar existencia de tablas
            String checkTables = "SHOW TABLES LIKE 'account_summary'";
            java.sql.ResultSet rs = stmt.executeQuery(checkTables);

            if (rs.next()) {
                String countSql = "SELECT COUNT(*) as total FROM account_summary";
                rs = stmt.executeQuery(countSql);
                if (rs.next()) {
                    int count = rs.getInt("total");
                    System.out.println("📊 Registros en account_summary: " + count);

                    //mostrar primeros 5 registros
                    if (count > 0) {
                        String selectSql = "SELECT * FROM account_summary LIMIT 5";
                        rs = stmt.executeQuery(selectSql);
                        while (rs.next()) {
                            System.out.println("Cuenta: " + rs.getString("account_number") +
                                    " | Min: " + rs.getInt("total_minutes") +
                                    " | Costo: Q" + rs.getDouble("total_cost") +
                                    " | Int: " + rs.getInt("international_calls") +
                                    " | Nac: " + rs.getInt("national_calls") +
                                    " | Loc: " + rs.getInt("local_calls"));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("Error en debug: " + ex.getMessage());
        }
    }

    //actualizar estado de productores y consumidores
    private void updateStatus() {
        if (processor == null) return;

        ConcurrentHashMap<String, String> producerStatus = processor.getProducerStatus();
        ConcurrentHashMap<String, String> consumerStatus = processor.getConsumerStatus();

        producerStatusArea.setText("=== PRODUCTORES ===\n\n");
        producerStatus.forEach((name, status) -> {
            producerStatusArea.append(name + ": " + status + "\n\n");
        });

        consumerStatusArea.setText("=== CONSUMIDORES ===\n\n");
        consumerStatus.forEach((name, status) -> {
            consumerStatusArea.append(name + ": " + status + "\n\n");
        });
    }

    //metodo main
    public static void main(String[] args) {
        try {
            // Forzar carga del driver JDBC de MySQL
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Driver JDBC cargado correctamente");
        } catch (ClassNotFoundException e) {
            System.err.println("No se encontro el driver JDBC de MySQL en el classpath");
            e.printStackTrace();
            return;
        }

        SwingUtilities.invokeLater(() -> {
            new CDRGUI();
        });
    }
}