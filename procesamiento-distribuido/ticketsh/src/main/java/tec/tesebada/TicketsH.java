package tec.tesebada;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TicketsH {
  private static final int ID_SUCURSAL = Integer.parseInt(System.getenv("STORE"));
  private static final Map<String, String> MONTHS = Map.ofEntries(
      Map.entry("ene", "Jan"),
      Map.entry("feb", "Feb"),
      Map.entry("mar", "Mar"),
      Map.entry("abr", "Apr"),
      Map.entry("may", "May"),
      Map.entry("jun", "Jun"),
      Map.entry("jul", "Jul"),
      Map.entry("ago", "Aug"),
      Map.entry("sep", "Sep"),
      Map.entry("oct", "Oct"),
      Map.entry("nov", "Nov"),
      Map.entry("dic", "Dec"));

  public static void main(String[] args) throws Exception {
    String url = "jdbc:postgresql://localhost:5432/clemente";
    String user = "postgres";
    String pass = "mysecretpassword";
    String inputFile = "TicketH.csv";

    String[] headers = { "Folio", "Fecha", "IdEstado", "IdCiudad", "IdTienda", "IdEstado" };
    List<String[]> csvData = Files.lines(Paths.get(inputFile))
        .skip(1)
        .map(line -> line.split(","))
        .filter(row -> Integer.parseInt(row[4]) == ID_SUCURSAL)
        .map(row -> {
          row[1] = normalizeDate(row[1]);
          return row;
        })
        .collect(Collectors.toCollection(LinkedList::new));
    csvData.addFirst(headers);

    StringWriter writer = new StringWriter();
    csvData.forEach(row -> writer.append(String.join(",", row)).append("\n"));

    try (Connection conn = DriverManager.getConnection(url, user, pass)) {
      CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      String sql = "COPY ticketsh (Folio, Fecha, IdEstado, IdCiudad, IdTienda, IdEmpleado) " +
          "FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')";

      try (Reader reader = new StringReader(writer.toString())) {
        long rows = copyManager.copyIn(sql, reader);
        System.out.println("Filas insertadas: " + rows);
      }
    }
  }

  private static String normalizeDate(String fecha) {
    for (var e : MONTHS.entrySet()) {
      if (fecha.contains(e.getKey())) {
        fecha = fecha.replace(e.getKey(), e.getValue());
        break;
      }
    }
    if (fecha.contains("31/Sep"))
      fecha = fecha.replace("31", "30");
    else if (fecha.contains("31/Apr"))
      fecha = fecha.replace("31", "30");
    else if (fecha.contains("31/Jun"))
      fecha = fecha.replace("31", "30");
    else if (fecha.contains("31/Nov"))
      fecha = fecha.replace("31", "30");
    return fecha;
  }
}
