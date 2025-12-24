import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class XMLValidator {

  public static void main(String[] args) {
    // 1. Validar que recibimos los dos argumentos
    if (args.length != 2) {
      System.err.println("Uso incorrecto.");
      System.err.println("Comando: java XmlValidator <ruta_xml> <ruta_dtd>");
      System.exit(1);
    }

    String xmlPath = args[0];
    String dtdPath = args[1];

    try {
      validateXml(xmlPath, dtdPath);
      System.out.println("El archivo XML es válido.");
    } catch (Exception e) {
      System.err.println("El archivo XML NO es válido.");
      System.err.println(e.getMessage());
    }
  }

  private static void validateXml(String xmlPath, String dtdPath)
      throws ParserConfigurationException, SAXException, IOException {

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setValidating(true);
    factory.setNamespaceAware(true);

    DocumentBuilder builder = factory.newDocumentBuilder();

    builder.setErrorHandler(new ErrorHandler() {
      @Override
      public void warning(SAXParseException exception) throws SAXException {
        System.out.println("Error: " + exception.getMessage());
      }

      @Override
      public void error(SAXParseException exception) throws SAXException {
        throw exception;
      }

      @Override
      public void fatalError(SAXParseException exception) throws SAXException {
        throw exception;
      }
    });

    builder.setEntityResolver(new EntityResolver() {
      @Override
      public InputSource resolveEntity(String publicId, String systemId)
          throws SAXException, IOException {
        if (systemId != null && systemId.endsWith(".dtd")) {
          return new InputSource(new FileInputStream(new File(dtdPath)));
        }
        return null;
      }
    });

    File xmlFile = new File(xmlPath);
    if (!xmlFile.exists()) {
      throw new IOException("No se encontró el archivo XML en: " + xmlPath);
    }

    builder.parse(xmlFile);
  }
}
