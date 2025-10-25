package tec.tesebada;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class App {
  private static final int ID_SUCURSAL = Integer.parseInt(System.getenv("STORE"));

  public static void main(String[] args) throws Exception {
    String localUrl = "jdbc:postgresql://localhost:5432/clemente";
    String remoteUrl = "jdbc:postgresql://db1:5432/clemente";
    String user = "postgres";
    String pass = "mysecretpassword";
    List<Integer> productos = new ArrayList<>();
    String sqlLocal = "SELECT td.idproducto " +
        "FROM ticketsd td " +
        "INNER JOIN ticketsh th ON td.ticket = th.folio " +
        "WHERE th.fecha >= '2021-01-01' AND th.fecha <= '2022-12-31' " +
        "GROUP BY td.idproducto " +
        "HAVING COUNT(DISTINCT DATE_TRUNC('month', th.fecha)) >= 12";

    try (Connection connLocal = DriverManager.getConnection(localUrl, user, pass);
        Statement stmtLocal = connLocal.createStatement();
        ResultSet rs = stmtLocal.executeQuery(sqlLocal)) {
      while (rs.next()) {
        productos.add(rs.getInt("idproducto"));
      }
    }

    try (Connection connRemote = DriverManager.getConnection(remoteUrl, user, pass)) {
      if (productos.isEmpty()) {
        System.out.println("No hay ningún producto que cumpla la condición");
        return;
      }

      String inClause = productos.stream().map(String::valueOf).collect(Collectors.joining(","));
      String sqlRemote = "UPDATE inventario " +
          "SET precio = precio + 1 " +
          "WHERE idsucursal = " + ID_SUCURSAL + " " +
          "AND idproducto IN (" + inClause + ") " +
          "RETURNING idproducto, precio";

      boolean flag = true;
      Scanner sc = new Scanner(System.in);

      while (flag) {
        try (Statement stmtRemote = connRemote.createStatement();
            ResultSet rsRemote = stmtRemote.executeQuery(sqlRemote)) {
          while (rsRemote.next()) {
            int id = rsRemote.getInt("idproducto");
            int price = rsRemote.getInt("precio");
            System.out.printf("ID Producto: %d, Nuevo Precio: %d%n", id, price);
          }
        }

        System.out.print("\n Volver a actualizar los precios: (s/n) ");
        String input = sc.nextLine();
        System.out.println();
        if (input.toLowerCase().equals("n")) {
          flag = false;
        }
      }

      sc.close();
    }
  }
}
