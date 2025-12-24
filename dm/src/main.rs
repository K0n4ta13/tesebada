use postgres::{Client, NoTls};
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "data.arff";
    let db_url = "postgresql://postgres:mysecretpassword@localhost/dm";

    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    let mut client = Client::connect(db_url, NoTls)?;

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

    let mut transaction = client.transaction()?;

    let statement = transaction.prepare("
        INSERT INTO conductores_examen (edad, licencia, estacion, genero, calificacion_teorica, calificacion_practica, aprobacion)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    ")?;

    for i in 0..400_000 {
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

        if i < 50_000 {
            writeln!(
                writer,
                "{},{},{},{},{},{},{}",
                edad, licencia, estacion, genero, cal_teorica, cal_practica, aprobacion_final
            )?;
        } else {
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
    }

    transaction.commit()?;

    Ok(())
}
