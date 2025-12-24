CREATE TABLE conductores_examen (
    edad                  INTEGER NOT NULL CHECK (edad BETWEEN 16 AND 65),
    licencia              TEXT    NOT NULL CHECK (licencia IN ('auto', 'chofer', 'moto')),
    estacion              TEXT    NOT NULL CHECK (estacion IN ('primavera', 'verano', 'otoño', 'invierno')),
    genero                TEXT    NOT NULL CHECK (genero IN ('masculino', 'femenino')),
    calificacion_teorica  INTEGER NOT NULL CHECK (calificacion_teorica BETWEEN 0 AND 10),
    calificacion_practica TEXT    NOT NULL CHECK (calificacion_practica IN ('alta', 'media', 'baja')),
    aprobacion            TEXT    NOT NULL CHECK (aprobacion IN ('si', 'no'))
);
