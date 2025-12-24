import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import java.io.File;

public class XMLWalker {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Error: Indica la ruta del archivo.");
      return;
    }

    try {
      File xmlFile = new File(args[0]);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

      dbFactory.setIgnoringComments(true);
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(xmlFile);

      doc.getDocumentElement().normalize();

      visitNode(doc.getDocumentElement(), "");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void visitNode(Node node, String indent) {
    if (node.getNodeType() == Node.ELEMENT_NODE) {
      System.out.println(indent + "ETIQUETA: <" + node.getNodeName() + ">");

      if (node.hasAttributes()) {
        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
          Node attr = attributes.item(i);
          System.out.println(indent + "   └── ATRIBUTO: " + attr.getNodeName() + " = \"" + attr.getNodeValue() + "\"");
        }
      }
    } else if (node.getNodeType() == Node.TEXT_NODE) {
      String text = node.getTextContent().trim();
      if (!text.isEmpty()) {
        System.out.println(indent + "   └── VALOR: \"" + text + "\"");
      }
    }

    NodeList nodeList = node.getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      visitNode(nodeList.item(i), indent + "    ");
    }
  }
}
