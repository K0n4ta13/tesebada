import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.helpers.DefaultHandler;
import java.io.File;

public class XML {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Error: Debes proporcionar la ruta del archivo como argumento.");
      return;
    }

    if (isWellFormed(args[0])) {
      System.out.println("El archivo está BIEN FORMADO.");
    } else {
      System.out.println("El archivo está MAL FORMADO.");
    }
  }

  public static boolean isWellFormed(String xmlFilePath) {
    try {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setValidating(false);
      factory.setNamespaceAware(true);

      SAXParser parser = factory.newSAXParser();
      parser.parse(new File(xmlFilePath), new DefaultHandler());
      return true;

    } catch (Exception e) {
      System.err.println(e.getMessage());
      return false;
    }
  }
}
