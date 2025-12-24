#set page("us-letter")
#set par(justify: true)
#set text(size: 12pt)
#set heading(numbering: "1.")
#set enum(indent: 1.5em, body-indent: 0.5em)

#show heading: set block(below: 16pt)
#show: page.with(header: none, footer: none)

#image("tec.jpeg")

#align(center)[
  #text(size: 26pt)[*Instituto Tecnológico de Culiacán*]

  #text(size: 22pt)[Tema Selecto de Base de Datos]

   #v(90pt)
  #text(size: 18pt)[
    *Alumno:* \
    Ramos Matunaga Raúl Alejandro

    #v(20pt)
    *Carrera:* \
    Ingeniería en Sistemas Computacionales

    #v(20pt)
    *Docente:* \
    Dr. García Gerardo Clemente
  ]
]

#pagebreak()

#show: page.with(
  header: stack(
    dir: ttb,
    spacing: 8pt,
    grid(
      columns: (1fr, 1fr),
      align(left)[Instituto Tecnológico de Culiacán],
      align(right)[Tema Selecto de Base de Datos]
    ),
    align(center)[XML]
  ),
  footer: grid(
    columns: (1fr, 1fr),
    align(left)[Dr. García Gerardo Clemente],
    align(right)[ISC]
  )
)

= Definición del esquema DTD (Document Type Definition)

== Descripción del dominio

El esquema modela una Biblioteca que se organiza en dos listas principales:

+ *authors:* Contiene el registro de los autores.
+ *books:* Contiene el catálogo de obras. Cada libro almacena su información (título, género, idioma) y, para indicar la autoría, hace referencia al código del escritor correspondiente.

== Etiquetas con restricciones

El esquema define las siguientes reglas lógicas para validar el documento:

+ `<author>`: Requiere obligatoriamente un atributo id de tipo ID. Este identificador debe ser único en todo el documento para permitir la integridad referencial.

+ `<book>`: Utiliza el atributo author de tipo IDREF para vincular la obra con un escritor registrado. Restringe los atributos isbn y release_year al tipo NMTOKEN y limita el atributo language a una lista enumerada de valores permitidos.

+ `<print_edition> / <digital_edition>`: Ambos elementos se definen como EMPTY, impidiendo contenido interno. Toda la información debe almacenarse estrictamente en sus atributos.

+ `<publisher>`: Se define como elemento EMPTY. Restringe el ingreso de datos exclusivamente a sus atributos.

+ `<genre>`: Se define como contenido de texto (\#PCDATA), destinado a contener una cadena de caracteres única con los géneros literarios.

== Formación del documento DTD

```bash
<!ELEMENT library (authors, books)>

<!ELEMENT authors (author+)>
<!ELEMENT author (#PCDATA)>

<!ATTLIST author id ID #REQUIRED>
<!ATTLIST author nationality CDATA #IMPLIED>

<!ELEMENT books (book+)>

<!ELEMENT book (title, genre, publisher, (print_edition | digital_edition), synopsis?)>

<!ATTLIST book isbn NMTOKEN #REQUIRED>
<!ATTLIST book language NMTOKEN #REQUIRED>
<!ATTLIST book release_year NMTOKEN #REQUIRED>
<!ATTLIST book author IDREF #REQUIRED>

<!ELEMENT title (#PCDATA)>
<!ATTLIST title original_title CDATA #IMPLIED>

<!ELEMENT genre (#PCDATA)>

<!ELEMENT publisher EMPTY>
<!ATTLIST publisher name CDATA #REQUIRED>
<!ATTLIST publisher country CDATA #IMPLIED>

<!ELEMENT print_edition EMPTY>
<!ATTLIST print_edition cover (hardcover | paperback) #REQUIRED>
<!ATTLIST print_edition pages NMTOKEN #REQUIRED>

<!ELEMENT digital_edition EMPTY>
<!ATTLIST digital_edition format (pdf | epub) #REQUIRED>

<!ELEMENT synopsis (#PCDATA)>
```

= Archivo XML

== Creación del archivo XML

Este documento implementa la estructura lógica definida en el esquema DTD, verificando la integridad referencial entre las entidades. El archivo se organiza en dos bloques secuenciales: primero, un catálogo maestro de autores (`<authors>`) donde se asignan identificadores únicos; y segundo, el inventario de libros (`<books>`). En cada registro se aplican las restricciones de validación diseñadas, tales como la asignación de autoría mediante IDREFs,

== Mostrar contenido del archivo XML

Extracto del archivo XML:

```bash
<library>
  <authors>
    <author id="JR" nationality="Mexicana">Juan Rulfo</author>
    <author id="YK" nationality="Japonesa">Yasunari Kawabata</author>
    <author id="FD" nationality="Rusa">Fiódor Dostoyevski</author>
  </authors>
  <books>
    <book isbn="978-84-376-0418-3" language="es" release_year="1955" author="JR">
      <title>Pedro Páramo</title>
      <genre>Novela</genre>
      <publisher name="Fondo de Cultura Económica" country="México"/>
      <print_edition cover="paperback" pages="132"/>
      <synopsis>Una de las obras maestras de la literatura hispanoamericana.</synopsis>
    </book>
    <book isbn="978-84-95501-14-1" language="jp" release_year="1964" author="YK">
      <title original_title="Utsukushisa to Kanashimi to">Lo bello y lo triste</title>
      <genre>Novela</genre>
      <publisher name="Emecé" country="Argentina"/>
      <print_edition cover="paperback" pages="224"/>
    </book>
    <book isbn="978-84-206-5381-5" language="ru" release_year="1880" author="FD">
      <title original_title="Brat'ya Karamazovy">Los hermanos Karamázov</title>
      <genre>Novela filosófica</genre>
      <publisher name="Alianza Editorial" country="España"/>
      <print_edition cover="paperback" pages="1120"/>
    </book>
  </books>
</library>
```

= Validar documento XML

== Bien formado

El siguiente código Java recibe un path a un archivo XML y valida si esta bien formado:

```java
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
```

#image("bien_formado.png")

La imagen evidencia una prueba funcional del programa en Java. Inicialmente, se analiza el archivo library.xml, confirmándose que se encuentra bien formado. Posteriormente, se realiza una modificación para introducir un error de sintaxis intencional y, al ejecutar el validador por segunda vez, el sistema detecta la inconsistencia reportando la ausencia de un delimitador en la etiqueta, concluyendo que el archivo está mal formado.

#pagebreak()

== Válido

Usando la herramienta Oxygen visualizamos el archivo XML y, a partir de su etiqueta DOCTYPE, busca el esquema DTD y lo valida automáticamente, mostrando en la parte inferior cómo este documento es válido de acuerdo al DTD.

#image("oxygen-1.png")

#v(1em)
#image("oxygen-2.png")

#v(1em)
Aquí, al remover el atributo language, podemos ver cómo, aunque está bien formado, no es válido de acuerdo al DTD y marca un error.

#image("oxygen-3.png")

= Mostrar contenido del documento XML

== Programa Java

El siguiente código Java recibe un path a un archivo XML, lo procesa y lo muestra en pantalla:

```java
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
```

== Mostrar los datos del archivo XML

=== Java

```bash
ETIQUETA: <library>
    ETIQUETA: <authors>
        ETIQUETA: <author>
           └── ATRIBUTO: id = "JR"
           └── ATRIBUTO: nationality = "Mexicana"
               └── VALOR: "Juan Rulfo"
                   ETIQUETA: <books>
        ETIQUETA: <book>
           └── ATRIBUTO: author = "JR"
           └── ATRIBUTO: isbn = "978-84-376-0418-3"
           └── ATRIBUTO: language = "es"
           └── ATRIBUTO: release_year = "1955"
            ETIQUETA: <title>
                   └── VALOR: "Pedro Páramo"
            ETIQUETA: <genre>
                   └── VALOR: "Novela"
            ETIQUETA: <publisher>
               └── ATRIBUTO: country = "México"
               └── ATRIBUTO: name = "Fondo de Cultura Económica"
            ETIQUETA: <print_edition>
               └── ATRIBUTO: cover = "paperback"
               └── ATRIBUTO: pages = "132"
            ETIQUETA: <synopsis>
                   └── VALOR: "Una de las obras maestras de la literatura hispanoamericana."
```

=== Oxygen

#image("oxygen-1.png")

= Tablas en Base de Datos con la información del archivo XML

```sql
CREATE TABLE authors (
    author_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    nationality VARCHAR(50)
);

CREATE TABLE books (
    isbn VARCHAR(20) PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    original_title VARCHAR(200),
    genre VARCHAR(50),
    language_code VARCHAR(10) NOT NULL,
    release_year INT NOT NULL,
    synopsis TEXT,
    publisher_name VARCHAR(100) NOT NULL,
    publisher_country VARCHAR(50),
    edition_type VARCHAR(10) CHECK (edition_type IN ('print', 'digital')),
    print_cover VARCHAR(20) CHECK (print_cover IN ('hardcover', 'paperback')),
    print_pages INT,
    digital_format VARCHAR(10) CHECK (digital_format IN ('pdf', 'epub')),
    author_id VARCHAR(10),
    CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

INSERT INTO buthors (author_id, name, nationality) VALUES
('JR', 'Juan Rulfo', 'Mexicana'),
('YK', 'Yasunari Kawabata', 'Japonesa'),
('FD', 'Fiódor Dostoyevski', 'Rusa');

INSERT INTO books (isbn, title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages, synopsis) 
VALUES ('978-84-376-0418-3', 'Pedro Páramo', 'Novela', 'es', 1955, 'JR', 'Fondo de Cultura Económica', 'México', 'print', 'paperback', 132, 'Una de las obras maestras de la literatura hispanoamericana.');
INSERT INTO books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-95501-14-1', 'Lo bello y lo triste', 'Utsukushisa to Kanashimi to', 'Novela', 'jp', 1964, 'YK', 'Emecé', 'Argentina', 'print', 'paperback', 224);
INSERT INTO books (isbn, title, original_title, genre, language_code, release_year, author_id, publisher_name, publisher_country, edition_type, print_cover, print_pages) 
VALUES ('978-84-206-5381-5', 'Los hermanos Karamázov', 'Brat''ya Karamazovy', 'Novela filosófica', 'ru', 1880, 'FD', 'Alianza Editorial', 'España', 'print', 'paperback', 1120);
```
