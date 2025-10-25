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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Inventory {

  record Product(String store, String product, String price, LocalDate date) {
  }

  record Sale(String ticket, String store, LocalDate date) {
  }

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yy", Locale.ENGLISH);
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
    String headersFile = "TicketH.csv";
    String detailsFile = "TicketD.csv";

    Map<String, Sale> sales = Files.lines(Paths.get(headersFile))
        .skip(1)
        .map(line -> line.split(","))
        .collect(Collectors.toMap(
            row -> row[0],
            row -> new Sale(row[0], row[4], normalizeDate(row[1]))));

    Map<String, Product> inventory = new HashMap<>();

    Files.lines(Paths.get(detailsFile))
        .skip(1)
        .map(line -> line.split(","))
        .forEach(row -> {
          String ticket = row[0];
          String product = row[1];
          String price = row[3];
          Sale sale = sales.get(ticket);

          if (sale != null) {
            String key = sale.store() + "-" + product;
            Product newProduct = new Product(sale.store(), product, price, sale.date());

            inventory.merge(key, newProduct, (v1, v2) -> v1.date().isAfter(v2.date()) ? v1 : v2);
          }
        });

    Random r = new Random();

    StringWriter writer = new StringWriter();
    writer.append("IdSucursal,IdProducto,Existencia,Minimo,Maximo,Precio\n");
    inventory.forEach((_, v) -> writer
        .append(v.store() + "," + v.product() + "," + r.nextInt(40, 71) + "," + r.nextInt(10, 31) + ","
            + r.nextInt(80, 101) + ","
            + v.price().substring(0, v.price().length() - 3).replaceFirst("\\s", "") + "\n"));

    try (Connection conn = DriverManager.getConnection(url, user, pass)) {
      CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      String sql = "COPY inventario (IdSucursal, IdProducto, Existencia, Minimo, Maximo, Precio) " +
          "FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')";

      try (Reader reader = new StringReader(writer.toString())) {
        long rows = copyManager.copyIn(sql, reader);
        System.out.println("Filas insertadas: " + rows);
      }
    }
  }

  private static LocalDate normalizeDate(String fecha) {
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

    return LocalDate.parse(fecha, FORMATTER);
  }
}
