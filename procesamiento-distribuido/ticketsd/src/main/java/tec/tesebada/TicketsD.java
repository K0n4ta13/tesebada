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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TicketsD {
  private static final int ID_SUCURSAL = Integer.parseInt(System.getenv("STORE"));

  public static void main(String[] args) throws Exception {
    String url = "jdbc:postgresql://localhost:5432/clemente";
    String user = "postgres";
    String pass = "mysecretpassword";
    String headersFile = "TicketH.csv";
    String detailsFile = "TicketD.csv";

    Set<String> tickets = Files.lines(Paths.get(headersFile))
        .skip(1)
        .map(line -> line.split(","))
        .filter(row -> Integer.parseInt(row[4]) == ID_SUCURSAL)
        .map(row -> row[0])
        .collect(Collectors.toSet());

    List<String[]> csvData = Files.lines(Paths.get(detailsFile))
        .skip(1)
        .map(line -> line.split(","))
        .filter(row -> tickets.contains(row[0]))
        .map(row -> {
          row[4] = row[4].substring(0, row[4].length() - 3).replaceFirst("\\$", "").replaceFirst("\\s", "");
          return row;
        })
        .map(row -> IntStream.range(0, row.length)
            .filter(i -> i != row.length - 2)
            .mapToObj(i -> row[i])
            .toArray(String[]::new))
        .collect(Collectors.toCollection(LinkedList::new));

    Set<String> seen = new HashSet<>();

    csvData = csvData.stream()
        .filter(row -> {
          String key = row[0] + "-" + row[1];
          return seen.add(key);
        })
        .collect(Collectors.toList());

    StringWriter writer = new StringWriter();
    writer.append("Ticket,IdProducto,Unidades,Precio\n");
    csvData.forEach(row -> writer.append(String.join(",", row)).append("\n"));

    try (Connection conn = DriverManager.getConnection(url, user, pass)) {
      CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      String sql = "COPY ticketsd (Ticket, IdProducto, Unidades, Precio) " +
          "FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')";

      try (Reader reader = new StringReader(writer.toString())) {
        long rows = copyManager.copyIn(sql, reader);
        System.out.println("Filas insertadas: " + rows);
      }
    }
  }
}
