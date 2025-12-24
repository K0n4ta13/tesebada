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
    align(center)[Proyecto de almacenes de datos]
  ),
  footer: grid(
    columns: (1fr, 1fr),
    align(left)[Dr. García Gerardo Clemente],
    align(right)[ISC]
  )
)

= Generación de archivo ARFF

La generación del archivo ARFF comienza añadiendo los atributos necesarios para definir la estructura. Luego, bajo la etiqueta `@DATA`, se inserta la información generada dinámicamente. Cada registro se crea siguiendo reglas específicas de validación y probabilidad antes de escribirse en el archivo.

```rust
let file_path = "data.arff";
let file = File::create(file_path)?;
let mut writer = BufWriter::new(file);

let mut rng = rand::rng();

writeln!(writer, "@RELATION conductores_examen\n")?;
writeln!(writer, "@ATTRIBUTE edad NUMERIC")?;
writeln!(writer, "@ATTRIBUTE licencia {{auto,chofer,moto}}")?;
writeln!(
    writer,
    "@ATTRIBUTE estacion {{primavera,verano,otoño,invierno}}"
)?;
writeln!(writer, "@ATTRIBUTE genero {{masculino,femenino}}")?;
writeln!(writer, "@ATTRIBUTE calificacion_teorica NUMERIC")?;
writeln!(
    writer,
    "@ATTRIBUTE calificacion_practica {{alta,media,baja}}"
)?;
writeln!(writer, "@ATTRIBUTE aprobacion {{si,no}}\n")?;
writeln!(writer, "@DATA")?;

let tipos_licencia = ["auto", "chofer", "moto"];
let estaciones = ["primavera", "verano", "otoño", "invierno"];
let generos = ["masculino", "femenino"];


for _ in 0..100_000 {
    let edad: i32 = rng.random_range(16..=65);
    let licencia = if edad < 18 {
        "auto"
    } else {
        tipos_licencia[rng.random_range(0..tipos_licencia.len())]
    };
    let estacion = estaciones[rng.random_range(0..estaciones.len())];
    let genero = generos[rng.random_range(0..generos.len())];

    let prob_teorica = if genero == "femenino" { 0.60 } else { 0.45 };

    let aprobo_teorico = rng.random_bool(prob_teorica);
    let cal_teorica: i32 = if aprobo_teorico {
        rng.random_range(6..=10)
    } else {
        rng.random_range(0..=5)
    };

    let prob_practico = match (aprobo_teorico, genero) {
        (true, "masculino") => 0.60,
        (false, "masculino") => 0.25,
        (true, "femenino") => 0.45,
        (false, "femenino") => 0.10,
        _ => unreachable!(),
    };

    let aprobo_practico = rng.random_bool(prob_practico);
    let cal_practico: i32 = if aprobo_practico {
        rng.random_range(6..=10)
    } else {
        rng.random_range(0..=5)
    };

    let (cal_practica, aprobo_practico) = match cal_practico {
        0..=5 => ("baja", false),
        6..=8 => ("media", true),
        _ => ("alta", true),
    };

    let probabilidad_aprobacion = match (aprobo_teorico, aprobo_practico) {
        (true, true) => 0.95,
        (true, false) => 0.40,
        (false, true) => 0.40,
        (false, false) => 0.00,
    };

    let aprobacion_final = if rng.random_bool(probabilidad_aprobacion) {
        "si"
    } else {
        "no"
    };

    writeln!(
        writer,
        "{},{},{},{},{},{},{}",
        edad, licencia, estacion, genero, cal_teorica, cal_practica, aprobacion_final
    )?;
```

== Datos

```
@RELATION conductores_examen

@ATTRIBUTE edad NUMERIC
@ATTRIBUTE licencia {auto,chofer,moto}
@ATTRIBUTE estacion {primavera,verano,otoño,invierno}
@ATTRIBUTE genero {masculino,femenino}
@ATTRIBUTE calificacion_teorica NUMERIC
@ATTRIBUTE calificacion_practica {alta,media,baja}
@ATTRIBUTE aprobacion {si,no}

@DATA
17,auto,invierno,femenino,3,media,si
58,auto,verano,masculino,9,media,si
46,auto,primavera,masculino,0,baja,no
51,chofer,verano,masculino,2,baja,no
34,auto,primavera,masculino,5,media,no
```

= Generación base de datos

La generación de la base de datos se ejecuta mediante una transacción SQL. Los datos se crean dinámicamente siguiendo reglas de validación específicas y se insertan registro a registro utilizando sentencias preparadas, concluyendo con un commit de la transacción.

```rust
let db_url = "postgresql://postgres:mysecretpassword@localhost/dm";
let mut client = Client::connect(db_url, NoTls)?;

let mut rng = rand::rng();

let tipos_licencia = ["auto", "chofer", "moto"];
let estaciones = ["primavera", "verano", "otoño", "invierno"];
let generos = ["masculino", "femenino"];

let mut transaction = client.transaction()?;

let statement = transaction.prepare("
    INSERT INTO conductores_examen (edad, licencia, estacion, genero, calificacion_teorica, calificacion_practica, aprobacion)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
")?;


for _ in 0..100_000 {
    let edad: i32 = rng.random_range(16..=65);
    let licencia = if edad < 18 {
        "auto"
    } else {
        tipos_licencia[rng.random_range(0..tipos_licencia.len())]
    };
    let estacion = estaciones[rng.random_range(0..estaciones.len())];
    let genero = generos[rng.random_range(0..generos.len())];

    let prob_teorica = if genero == "femenino" { 0.60 } else { 0.45 };

    let aprobo_teorico = rng.random_bool(prob_teorica);
    let cal_teorica: i32 = if aprobo_teorico {
        rng.random_range(6..=10)
    } else {
        rng.random_range(0..=5)
    };

    let prob_practico = match (aprobo_teorico, genero) {
        (true, "masculino") => 0.60,
        (false, "masculino") => 0.25,
        (true, "femenino") => 0.45,
        (false, "femenino") => 0.10,
        _ => unreachable!(),
    };

    let aprobo_practico = rng.random_bool(prob_practico);
    let cal_practico: i32 = if aprobo_practico {
        rng.random_range(6..=10)
    } else {
        rng.random_range(0..=5)
    };

    let (cal_practica, aprobo_practico) = match cal_practico {
        0..=5 => ("baja", false),
        6..=8 => ("media", true),
        _ => ("alta", true),
    };

    let probabilidad_aprobacion = match (aprobo_teorico, aprobo_practico) {
        (true, true) => 0.95,
        (true, false) => 0.40,
        (false, true) => 0.40,
        (false, false) => 0.00,
    };

    let aprobacion_final = if rng.random_bool(probabilidad_aprobacion) {
        "si"
    } else {
        "no"
    };

    transaction.execute(
        &statement,
        &[
            &edad,
            &licencia,
            &estacion,
            &genero,
            &cal_teorica,
            &cal_practica,
            &aprobacion_final,
        ],
    )?;
}

transaction.commit()?;
```

== Datos

```
  41 | moto   | primavera | masculino | 1  | alta  | no
  62 | auto   | invierno  | femenino  | 6  | baja  | si
  44 | moto   | invierno  | femenino  | 10 | baja  | no
  46 | chofer | otoño     | femenino  | 9  | media | si
  63 | auto   | primavera | masculino | 8  | media | si
```

= Reglas de Generación de Datos

La generación de los registros se rige por las siguientes reglas lógicas y probabilísticas para asegurar la coherencia de los datos:

- *Edad y Licencia:*
  - La edad se genera aleatoriamente en el rango $[16, 65]$.
  - Los menores de $18$ años están restringidos exclusivamente a la licencia de *automovilista*.

- *Probabilidades por Género:*
  - *Femenino:* Posee un $+15\%$ de probabilidad adicional de aprobar el examen teórico.
  - *Masculino:* Posee un $+15\%$ de probabilidad adicional de aprobar el examen práctico.

- *Calificaciones:*
  - *Teórica:* Valor numérico en el rango $0-10$.
  - *Práctica:* Valor numérico en el rango $0-10$, categorizado como:
    - *Baja* ($0-5$), *Media* ($6-8$) y *Alta* ($9-10$).
    - *Condicionante:* Si se reprueba la teoría, la probabilidad de aprobar la práctica se reduce al $10\%$.

- *Aprobación de Licencia:*
  La probabilidad de emisión final depende de los exámenes aprobados:
  - *Ambos aprobados:* $95\%$ de probabilidad.
  - *Uno aprobado:* $40\%$ de probabilidad.
  - *Ninguno aprobado:* $0\%$ de probabilidad.

= Flujo de Conocimiento

== Integración de Datos Híbrida

Este flujo unifica información de dos orígenes distintos: carga un archivo ARFF y extrae registros de una base de datos PostgreSQL. Finalmente, el nodo Concatenate fusiona ambas fuentes, creando una tabla maestra consolidada para su posterior análisis.

#image("workflow-1.png")

== Evaluación y Captura de Modelos

Este flujo ejecuta un bucle iterativo para entrenar y validar múltiples árboles de decisión. En cada iteración, entrena un modelo, mide su precisión con el Scorer y, simultáneamente, convierte el árbol en un objeto de datos mediante Model to Cell. Finalmente, el Column Appender une la métrica de precisión con el modelo correspondiente, resultando en una tabla acumulada que permite comparar el rendimiento de cada árbol generado.

#image("workflow-2.png")

== Selección y Visualización de Modelos

Este segmento final filtra los resultados para conservar únicamente los mejores candidatos. Posteriormente, inicia un bucle que procesa cada fila individualmente: el nodo Cell to Model restaura el modelo guardado en la tabla a su formato original para generar su gráfico interactivo, permitiendo así inspeccionar visualmente los árboles ganadores.

#image("workflow-3.png")
