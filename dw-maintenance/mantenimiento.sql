CREATE TABLE Ventas (
    idVenta    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idCliente  INTEGER NOT NULL,
    fecha      DATE    NOT NULL,
    importe    MONEY   NOT NULL,
    idSucursal INTEGER NOT NULL,
    idEmpleado INTEGER NOT NULL,
    idCiudad   INTEGER
);

CREATE TABLE VentasUSA (
    idVenta    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idCliente  INTEGER NOT NULL,
    fecha      DATE    NOT NULL,
    importe    MONEY   NOT NULL,
    idSucursal INTEGER NOT NULL,
    idEmpleado INTEGER NOT NULL,
    idCiudad   INTEGER
);

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE TABLE VistaMaterializada (
    idCliente      INTEGER NOT NULL,
    fecha          DATE    NOT NULL,
    idSucursal     INTEGER NOT NULL,
    cantidad       INTEGER NOT NULL,
    total_nacional MONEY   NOT NULL,
    total_usa      MONEY   NOT NULL,
    max_usa        MONEY   NOT NULL,
    vInicio        INTEGER NOT NULL,
    vFin           INTEGER,
    operacion      TEXT    NOT NULL,
    PRIMARY KEY (idCliente, fecha, idSucursal, vInicio)
);

CREATE TABLE DW_Ventas (
    idCliente    INTEGER NOT NULL,
    fecha        DATE    NOT NULL,
    idSucursal   INTEGER NOT NULL,
    DWSumImporte MONEY   NOT NULL,
    DWCount      INTEGER NOT NULL,
    PRIMARY KEY (idCliente, fecha, idSucursal)
);

CREATE TABLE DW_VentasUSA (
    idCliente    INTEGER NOT NULL,
    fecha        DATE    NOT NULL,
    idSucursal   INTEGER NOT NULL,
    DWSumImporte MONEY   NOT NULL,
    DWMaxImporte MONEY   NOT NULL,
    DWCount      INTEGER NOT NULL,
    PRIMARY KEY (idCliente, fecha, idSucursal)
);

CREATE TABLE DW_VentasUSA_Asociada (
    idCliente  INTEGER NOT NULL,
    fecha      DATE    NOT NULL,
    idSucursal INTEGER NOT NULL,
    atrMinMax  TEXT    NOT NULL,
    atrValor   MONEY   NOT NULL,
    atrVeces   INTEGER NOT NULL,
    PRIMARY KEY (idCliente, fecha, idSucursal, atrValor)
);

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE TABLE DW_Version (
    version INTEGER NOT NULL,
    activa  BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION fn_obtener_version_activa()
RETURNS INTEGER AS $$
DECLARE
    v_version_fila RECORD;
    v_version_final INTEGER;
BEGIN
    SELECT * INTO v_version_fila
    FROM DW_Version
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
        INSERT INTO DW_Version (version, activa)
        VALUES (1, TRUE)
        RETURNING version INTO v_version_final;

    ELSIF v_version_fila.activa = TRUE THEN
        v_version_final := v_version_fila.version;

    ELSE
        v_version_final := v_version_fila.version + 1;

        UPDATE DW_Version
        SET
            version = v_version_final,
            activa = TRUE;
    END IF;

    RETURN v_version_final;
END;
$$ LANGUAGE plpgsql;

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE OR REPLACE FUNCTION fn_dw_ventas_mantenimiento()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO DW_Ventas (idCliente, fecha, idSucursal, DWSumImporte, DWCount)
        VALUES (NEW.idCliente, NEW.fecha, NEW.idSucursal, NEW.importe, 1)
        ON CONFLICT (idCliente, fecha, idSucursal)
        DO UPDATE SET DWSumImporte = DW_Ventas.DWSumImporte + EXCLUDED.DWSumImporte,
            DWCount = DW_Ventas.DWCount + 1;

        PERFORM fn_actualizar_vista_materializada(NEW.idCliente, NEW.fecha, NEW.idSucursal, 'INSERT');

        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        UPDATE DW_Ventas
        SET DWSumImporte = DWSumImporte - OLD.importe,
            DWCount = DWCount - 1
        WHERE idCliente = OLD.idCliente AND fecha = OLD.fecha AND idSucursal = OLD.idSucursal;

        DELETE FROM DW_Ventas
        WHERE idCliente = OLD.idCliente AND fecha = OLD.fecha 
          AND idSucursal = OLD.idSucursal AND DWSumImporte <= 0::MONEY AND DWCount <= 0;

        PERFORM fn_actualizar_vista_materializada(OLD.idCliente, OLD.fecha, OLD.idSucursal, 'DELETE');

        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        UPDATE DW_Ventas
        SET DWSumImporte = DWSumImporte + (NEW.importe - OLD.importe)
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal;

        PERFORM fn_actualizar_vista_materializada(NEW.idCliente, NEW.fecha, NEW.idSucursal, 'UPDATE');

        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_ventas_mantenimiento_dw
AFTER INSERT OR UPDATE OR DELETE
ON Ventas
FOR EACH ROW
EXECUTE FUNCTION fn_dw_ventas_mantenimiento();

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE OR REPLACE FUNCTION fn_dw_ventasusa_mantenimiento()
RETURNS TRIGGER AS $$
DECLARE
    v_nuevo_max MONEY;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO DW_VentasUSA (idCliente, fecha, idSucursal, DWSumImporte, DWMaxImporte, DWCount)
        VALUES (NEW.idCliente, NEW.fecha, NEW.idSucursal, NEW.importe, NEW.importe, 1)
        ON CONFLICT (idCliente, fecha, idSucursal)
        DO UPDATE
        SET DWSumImporte = DW_VentasUSA.DWSumImporte + EXCLUDED.DWSumImporte,
            DWMaxImporte = GREATEST(DW_VentasUSA.DWMaxImporte, EXCLUDED.DWMaxImporte),
            DWCount = DW_VentasUSA.DWCount + 1;

        INSERT INTO DW_VentasUSA_Asociada (idCliente, fecha, idSucursal, atrMinMax, atrValor, atrVeces)
        VALUES (NEW.idCliente, NEW.fecha, NEW.idSucursal, 'importe', NEW.importe, 1)
        ON CONFLICT (idCliente, fecha, idSucursal, atrValor)
        DO UPDATE SET atrVeces = DW_VentasUSA_Asociada.atrVeces + 1;

        PERFORM fn_actualizar_vista_materializada(NEW.idCliente, NEW.fecha, NEW.idSucursal, 'INSERT');

        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        UPDATE DW_VentasUSA
        SET DWSumImporte = DWSumImporte - OLD.importe,
            DWCount = DWCount - 1
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal;

        UPDATE DW_VentasUSA_Asociada
        SET atrVeces = atrVeces - 1
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal
          AND atrValor = OLD.importe;

        DELETE FROM DW_VentasUSA_Asociada
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal
          AND atrVeces <= 0;

        SELECT MAX(atrValor) INTO v_nuevo_max
        FROM DW_VentasUSA_Asociada
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal
          AND atrMinMax = 'importe';

        UPDATE DW_VentasUSA
        SET DWMaxImporte = COALESCE(v_nuevo_max, 0::MONEY)
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal;

        DELETE FROM DW_VentasUSA
        WHERE idCliente = OLD.idCliente
          AND fecha = OLD.fecha
          AND idSucursal = OLD.idSucursal
          AND DWSumImporte <= 0::MONEY
          AND DWCount <= 0;

        PERFORM fn_actualizar_vista_materializada(OLD.idCliente, OLD.fecha, OLD.idSucursal, 'DELETE');

        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        UPDATE DW_VentasUSA
        SET DWSumImporte = DWSumImporte + (NEW.importe - OLD.importe),
            DWMaxImporte = GREATEST(DWMaxImporte, NEW.importe)
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal;

        UPDATE DW_VentasUSA_Asociada
        SET atrVeces = atrVeces - 1
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal
          AND atrValor = OLD.importe;

        DELETE FROM DW_VentasUSA_Asociada
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal
          AND atrVeces <= 0;

        INSERT INTO DW_VentasUSA_Asociada (idCliente, fecha, idSucursal, atrMinMax, atrValor, atrVeces)
        VALUES (NEW.idCliente, NEW.fecha, NEW.idSucursal, 'importe', NEW.importe, 1)
        ON CONFLICT (idCliente, fecha, idSucursal, atrValor)
        DO UPDATE SET atrVeces = DW_VentasUSA_Asociada.atrVeces + 1;

        SELECT MAX(atrValor) INTO v_nuevo_max
        FROM DW_VentasUSA_Asociada
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal
          AND atrMinMax = 'importe';

        UPDATE DW_VentasUSA
        SET DWMaxImporte = COALESCE(v_nuevo_max, 0::MONEY)
        WHERE idCliente = NEW.idCliente
          AND fecha = NEW.fecha
          AND idSucursal = NEW.idSucursal;

        PERFORM fn_actualizar_vista_materializada(NEW.idCliente, NEW.fecha, NEW.idSucursal, 'UPDATE');

        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_ventasusa_mantenimiento_dw
AFTER INSERT OR UPDATE OR DELETE
ON VentasUSA
FOR EACH ROW
EXECUTE FUNCTION fn_dw_ventasusa_mantenimiento();

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE OR REPLACE FUNCTION fn_actualizar_vista_materializada(
    k_cliente INTEGER,
    k_fecha DATE,
    k_sucursal INTEGER,
    k_operacion TEXT
)
RETURNS VOID AS $$
DECLARE
    v_nac_sum MONEY;
    v_nac_count INTEGER;
    v_usa_sum MONEY;
    v_usa_max MONEY;
    v_usa_count INTEGER;
    v_nueva_version INTEGER;
    v_operacion_final TEXT;
    prev_record VistaMaterializada%ROWTYPE;
BEGIN
    v_nueva_version := fn_obtener_version_activa();

    SELECT DWSumImporte, DWCount
    INTO v_nac_sum, v_nac_count
    FROM DW_Ventas
    WHERE idCliente = k_cliente
      AND fecha = k_fecha
      AND idSucursal = k_sucursal;

    SELECT DWSumImporte, DWMaxImporte, DWCount
    INTO v_usa_sum, v_usa_max, v_usa_count
    FROM DW_VentasUSA
    WHERE idCliente = k_cliente
      AND fecha = k_fecha
      AND idSucursal = k_sucursal;

    UPDATE VistaMaterializada
    SET vFin = v_nueva_version - 1
    WHERE idCliente = k_cliente
      AND fecha = k_fecha
      AND idSucursal = k_sucursal
      AND vFin IS NULL
      AND vInicio < v_nueva_version
    RETURNING * INTO prev_record;

    IF v_nac_count IS NOT NULL AND v_usa_count IS NOT NULL THEN
        v_operacion_final := CASE
            WHEN k_operacion = 'DELETE' THEN 'UPDATE'
            ELSE k_operacion
        END;

        INSERT INTO VistaMaterializada (
            idCliente, fecha, idSucursal,
            cantidad, total_nacional, total_usa, max_usa,
            vInicio, operacion, vFin
        )
        VALUES (
            k_cliente, k_fecha, k_sucursal,
            (v_nac_count + v_usa_count),
            v_nac_sum, v_usa_sum, v_usa_max,
            v_nueva_version, v_operacion_final, NULL
        )
        ON CONFLICT (idCliente, fecha, idSucursal, vInicio)
        DO UPDATE SET
            cantidad = EXCLUDED.cantidad,
            total_nacional = EXCLUDED.total_nacional,
            total_usa = EXCLUDED.total_usa,
            max_usa = EXCLUDED.max_usa,
            operacion = 'UPDATE',
            vFin = NULL;

    ELSIF k_operacion = 'DELETE' THEN
        IF FOUND AND prev_record.vInicio IS NOT NULL AND prev_record.vInicio != v_nueva_version THEN
            INSERT INTO VistaMaterializada (
                idCliente, fecha, idSucursal,
                cantidad, total_nacional, total_usa, max_usa,
                vInicio, operacion, vFin
            )
            VALUES (
                prev_record.idCliente,
                prev_record.fecha,
                prev_record.idSucursal,
                prev_record.cantidad,
                prev_record.total_nacional,
                prev_record.total_usa,
                prev_record.max_usa,
                v_nueva_version,
                'DELETE',
                NULL
            )
            ON CONFLICT (idCliente, fecha, idSucursal, vInicio)
            DO UPDATE SET operacion = 'DELETE';
        ELSE
            UPDATE VistaMaterializada
            SET operacion = 'DELETE'
            WHERE idCliente = k_cliente
              AND fecha = k_fecha
              AND idSucursal = k_sucursal
              AND vInicio = v_nueva_version;
        END IF;
    END IF;

END;
$$ LANGUAGE plpgsql;

-- ==================================================================================== --
-- ==================================================================================== --
-- ==================================================================================== --

CREATE OR REPLACE FUNCTION obtener_ventas(q_version INT)
RETURNS TABLE(
    idCliente      INTEGER,
    fecha          DATE,
    idSucursal     INTEGER,
    cantidad       INTEGER,
    total_nacional MONEY,
    total_usa      MONEY,
    max_usa        MONEY
) AS $$
BEGIN
    UPDATE dw_version
    SET activa = false
    WHERE activa = true;

    RETURN QUERY
    SELECT
        rv.idCliente,
        rv.fecha,
        rv.idSucursal,
        rv.cantidad,
        rv.total_nacional,
        rv.total_usa,
        rv.max_usa
    FROM
        resumenventas AS rv
    WHERE
        rv.vinicio <= q_version 
        AND (rv.vfin >= q_version OR rv.vfin IS NULL)
        AND rv.operacion != 'DELETE';

END;
$$ LANGUAGE plpgsql;
