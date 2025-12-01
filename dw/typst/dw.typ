#set page("us-letter")
#set par(justify: true)
#set text(size: 12pt)

#show heading: set block(below: 16pt)
#show: page.with(header: none, footer: none)

#image("tec.jpeg")

#align(center)[
  #text(size: 26pt)[*Instituto Tecnológico de Culiacán*]

  #text(size: 22pt)[Tema Selecto de Base de Datos]

   #v(90pt)
  #text(size: 18pt)[
    *Alumnos:* \
    Abitia Burgos Julio Alejandro \
    Ramos Matunaga Raúl Alejandro \
    Torres Lizárraga Yair

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
    align(center)[Proyecto de almacenes de datos]
  ),
  footer: grid(
    columns: (1fr, 1fr),
    align(left)[Dr. García Gerardo Clemente],
    align(right)[ISC]
  )
)

= Limpieza de Datos

Se definen las structs `Record` y `Transaction` para modelar la estructura de datos de las filas de los archivos solicitudes.csv y transacciones.csv, respectivamente.

#grid(
  columns: (1fr, 1fr),
  [
    ```rust
    struct Record {
      red: String,
      client_name: String,
      country: String,
      application_date: NaiveDate,
      employee_name: String,
      placed: String,
      card_id: u32,
      target: u32,
      gender: String,
    }
    ```
  ],
  [
    ```rust
    struct Transaction {
        card_id: u32,
        date: NaiveDate,
        country: String,
        import: f64,
        usd: f64,
    }
    ```
  ]
)

#v(12pt)
Tanto Record como Transaction implementan un método asociado from_str. Esta función actúa como un constructor que parsea una línea de texto completa del CSV y la convierte en una instancia de la struct correspondiente, asignando cada parte al campo apropiado.

```rust
impl Record {
    fn from_str(s: &str) -> Self {
        let parts = s.split(',').collect::<Vec<_>>();
        Self {
            red: parts[0].to_string(),
            client_name: parts[1].to_string(),
            country: parts[2].to_string(),
            application_date: normalize_date(parts[3]),
            employee_name: parts[4].to_string(),
            placed: parts[5].to_string(),
            card_id: parts[6].parse().unwrap(),
            target: parts[7].parse().unwrap(),
            gender: parts[8].to_string(),
        }
    }
}

impl Transaction {
    fn from_str(s: &str) -> Self {
        let parts = s.split(',').collect::<Vec<_>>();
        Self {
            card_id: parts[0].parse().unwrap(),
            date: normalize_date(parts[1]),
            country: parts[2].to_string(),
            import: parts[3].parse().unwrap(),
            usd: 0f64,
        }
    }
}
```

#v(12pt)
Para la operación inversa, ambas structs implementan el trait `Display`. Esto permite formatear una instancia de la struct directamente como una cadena de texto, con sus campos separados por comas, lo cual facilita su escritura directa en los archivos CSV de salida.

```rust
impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{}",
            self.red,
            self.client_name,
            self.country,
            self.application_date.format("%-d/%-m/%-Y"),
            self.employee_name,
            self.placed,
            self.card_id,
            self.target,
            self.gender
        )
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{}",
            self.card_id,
            self.date.format("%-d/%-m/%-Y"),
            self.country,
            self.import,
            self.usd
        )
    }
}
```

#v(12pt)
Se implementa una función de utilidad, normalize_date, responsable de la normalización de las cadenas de fecha al tipo NaiveDate. Esta función maneja fechas anómalas mediante un bucle iterativo que decrementa el día hasta encontrar una fecha válida, asegurando la integridad de los datos temporales.

```rust
fn normalize_date(date: &str) -> NaiveDate {
    let parts = date.split('/').collect::<Vec<_>>();
    let mut day = parts[0].parse::<u32>().unwrap();
    let month = parts[1].parse::<u32>().unwrap();
    let year = parts[2].parse::<i32>().unwrap();

    loop {
        match NaiveDate::from_ymd_opt(year, month, day) {
            Some(date) => return date,
            None => day -= 1,
        }
    }
}
```

#v(12pt)
La función `main` orquesta todo el proceso en dos partes:

Primero lee el archivo de solicitudes. Normaliza los datos de los empleados para que todos los registros asociados a un mismo empleado en una misma fecha usen el objetivo más alto reportado para esa fecha. Luego, escribe los datos limpios en un nuevo archivo de salida.

```rust
let file = std::fs::File::open("solicitudes.csv").unwrap();
let file = std::io::BufReader::new(file);
let mut file = file.lines();

let mut seen: HashMap<u128, (String, HashMap<NaiveDate, u32>)> = HashMap::new();

let headers = file.next().map(|r| r.unwrap()).unwrap();

let data = file
    .map(Result::unwrap)
    .map(|row| Record::from_str(&row))
    .map(|row| {
        let hash: u128 = row
            .employee_name
            .split(' ')
            .map(|s| {
                let mut hasher = DefaultHasher::new();
                s.hash(&mut hasher);
                hasher.finish() as u128
            })
            .sum();
        seen.entry(hash)
            .and_modify(|(name, targets)| {
                targets
                    .entry(row.application_date)
                    .and_modify(|target| {
                        if *target < row.target {
                            *name = row.employee_name.clone();
                            *target = row.target
                        }
                    })
                    .or_insert(row.target);
            })
            .or_insert_with(|| {
                (
                    row.employee_name.clone(),
                    HashMap::from([(row.application_date, row.target)]),
                )
            });
        (hash, row)
    })
    .collect::<LinkedList<(_, _)>>();
let data = data
    .into_iter()
    .map(|(hash, mut row)| {
        let (name, targets) = seen.get(&hash).unwrap();
        if row.employee_name != *name {
            row.employee_name = name.clone();
        }
        let target = targets.get(&row.application_date).unwrap();
        if row.target != *target {
            row.target = *target;
        }
        row
    })
    .collect::<LinkedList<_>>();

let mut output_file = File::create("solicitudes-clean.csv").unwrap();
writeln!(output_file, "{headers}").unwrap();
data.iter()
    .for_each(|r| writeln!(output_file, "{r}").unwrap());
```

#v(12pt)
A continuación lee el archivo transacciones. Para cada transacción, determina la moneda local y consulta una API externa para obtener el tipo de cambio histórico a USD correspondiente a la fecha de la transacción. Añade el importe convertido en una nueva columna y guarda el resultado en el archivo de salida.

```rust
let file = std::fs::File::open("transacciones.csv").unwrap();
let file = std::io::BufReader::new(file);
let mut file = file.lines();

let mut headers = file.next().map(|r| r.unwrap()).unwrap();
headers.push_str("Importe USD");
let mut output_file = File::create("transacciones-clean.csv").unwrap();
writeln!(output_file, "{headers}").unwrap();

file
    .map(Result::unwrap)
    .map(|row| Transaction::from_str(&row))
    .map(|mut tran| {
        let currency_code = match tran.country.as_str() {
            "Mexico" => "MXN",
            "Japon" => "JYP",
            "Brasil" => "BRL",
            "Francia" => "EUR",
            "USA" => "USD",
            _ => unreachable!(),
        };
        let dolars = match currency_code {
            "USD" => tran.import,
            code => {
                let year = tran.date.year();
                let month = tran.date.month();
                let day = tran.date.day();

                let url = format!("https://v6.exchangerate-api.com/v6/b52d3246c5b48151bae8546a/history/{code}/{year}/{month}/{day}");
                let currency: Currency = reqwest::blocking::get(url).unwrap().json().unwrap();
                currency.conversion_rates.get(code).unwrap() * tran.import
            }
        };
        tran.usd = dolars;
        tran
})
.for_each(|tran| writeln!(output_file, "{tran}").unwrap());
```
#pagebreak()

= Dashboards

El primer dashboard detalla el desempeño de los empleados en relación con sus objetivos de colocación de tarjetas. Los indicadores clave de rendimiento revelan los empleados que no alcanzaron su meta diaria y los empleados que sí cumplieron su objetivo.

#image("dashboard-1-1.png")

Asimismo, para facilitar un análisis dinámico e interactivo, se ha incorporado un slicer que permite filtrar los datos de los empleados con base en la red de tarjetas asignada. A modo de ejemplo, al seleccionar la red "MasterCard", la visualización se actualiza para mostrar exclusivamente la información correspondiente a los empleados responsables de la colocación de tarjetas de dicha red.

#image("dashboard-1-2.png")

En la sección inferior, se presenta una matriz que desglosa la información a un nivel granular. Esta tabla detalla el día asignado para la colocación, el total de tarjetas colocadas, el objetivo diario estipulado y la desviación resultante con respecto a la meta.

#image("dashboard-1-3.png")

#pagebreak()

El dashboard presenta un análisis detallado del desempeño individual de cada empleado. La vista principal consolida las métricas clave, incluyendo el nombre del colaborador, el volumen total de tarjetas colocadas, el objetivo acumulado y la desviación neta resultante. Adicionalmente, el panel incorpora un indicador de estado que informa textualmente si el empleado "Sí cumplió" o "No cumplió" con su meta agregada.

#image("dashboard-1-4.png")

El segundo dashboard ofrece un análisis detallado de los importes generados por país, permitiendo cuantificar su contribución al volumen total.

El panel principal expone el perfil demográfico y económico de cada nación, incluyendo datos relevantes como su capital, idioma, nivel de ingresos, PIB y superficie, con el fin de contextualizar los resultados financieros. Una tabla en la sección inferior desglosa los importes totales por red de tarjeta, lo cual facilita un análisis comparativo entre países y redes.

Un gráfico circular complementa esta vista, ilustrando la participación porcentual de cada país sobre el importe total. Paralelamente, un gráfico de barras identifica a los empleados con la mayor contribución económica.

Finalmente, un mapa geográfico interactivo permite visualizar la distribución de los importes a escala global, brindando una perspectiva clara sobre las regiones con mayor desempeño financiero. Este dashboard es una herramienta clave para la evaluación del rendimiento por país, la detección de oportunidades de crecimiento y la optimización de estrategias comerciales a nivel internacional.

#image("dashboard-2-1.png")

El dashboard desglosa los importes generados, segmentándolos por país y red de tarjeta, e indicando los totales correspondientes.

La herramienta permite una segmentación secundaria; al seleccionar un país específico, los datos se desagregan por semestre. Esto facilita el análisis de la evolución temporal del rendimiento. Al desplegar la información semestral, se exponen los importes correspondientes a cada mes dentro del período seleccionado.

Si se requiere un nivel de detalle superior, el dashboard permite una profundización adicional, mostrando las fechas exactas en las que se registraron los importes y el monto específico de cada uno.

Dicha estructura posibilita la identificación precisa de los montos generados por cada combinación de país y red, facilitando una comparativa directa del desempeño económico y la detección de las regiones o redes que aportan mayor valor a la organización.

#image("dashboard-2-2.png")

= Conclusiones

*Abitia Burgos Julio Alejandro:* \
En este proyecto entre las cosas que mas me llevo es el uso de la herramienta Power BI ya que aprendí a manejarla de una buena manera, al igual a la hora de manejar el lenguaje DAX que es fácil de usarlo y te ayuda hacer un mejor trabajo a la hora de crear medidas y tablas. Al igual uno de los aprendizajes que me llevo de este proyecto es la importancia de hacer la limpieza de los datos, ya que una vez que la información está limpia la puedes manipular como tú quieras y a la hora de usarla puede representar muchas cosas.

#v(12pt)
*Ramos Matunaga Raúl Alejandro:* \
Este proyecto fue mi primer acercamiento a Power BI y al lenguaje de expresiones DAX, lo que me permitió iniciarme en el área de ciencia de datos, algo que desconocía completamente y que me hizo ver su importancia, así como conocer las capacidades de Power BI, la facilidad para la creación de complejos dashboards y su importancia a la hora de llevar a cabo la toma de decisiones con base en datos.

#v(12pt)
*Yair Torres Lizárraga:* \
Este proyecto me permitió ver la importancia de la limpieza de los datos y cómo esta puede tener un gran impacto en la generación de resultados confiables. Aprendí a utilizar la herramienta Power BI, comprendiendo su integración con el lenguaje DAX, el cual facilitó la creación de medidas y columnas calculadas. \
Además, pude notar las ventajas que ofrece DAX, al permitir reducir la cantidad de código necesario y brindar soluciones más limpias, dinámicas y reutilizables para el análisis de la información.

