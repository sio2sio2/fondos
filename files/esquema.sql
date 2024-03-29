--
-- SCRAPERS disponibles
--
CREATE TABLE IF NOT EXISTS tScraper (
   scraper        CHAR(20)       PRIMARY KEY,
   desc           VARCHAR(256)
);


--
-- FONDO de inversión
--
CREATE TABLE IF NOT EXISTS tFondo (
   isin           CHAR(12)       PRIMARY KEY,
   nombre         VARCHAR(75)    NOT NULL,
   alias          VARCHAR(30),
   gestora        VARCHAR(50),
   moneda         CHAR(3)        NOT NULL,
   riesgo         INTEGER,
   scraper        CHAR(20),
   activo         BOOLEAN,        -- Si se desea obtener cotizaciones del fondo.

   CONSTRAINT fk_fon_scr FOREIGN KEY(scraper) REFERENCES tScraper(scraper) ON DELETE SET NULL ON UPDATE CASCADE
);


CREATE TABLE IF NOT EXISTS tScraperData (
   scraper        CHAR(20)       NOT NULL,
   isin           CHAR(12)       NOT NULL,
   data           VARCHAR(256),  -- Dato del fondo que necesita el scraper para obtener la cotización.

   PRIMARY KEY(scraper, isin),
   CONSTRAINT fk_fon_plu FOREIGN KEY(isin) REFERENCES tFondo(isin) ON DELETE CASCADE ON UPDATE CASCADE,
   CONSTRAINT fk_scr_plu FOREIGN KEY(scraper) REFERENCES tScraper(scraper) ON DELETE CASCADE ON UPDATE CASCADE
);


--
-- Añade a tFondo el campo scraper_data con el dato necesario
-- para extraer el fondo con el scraper.
-- 
CREATE VIEW IF NOT EXISTS Fondo AS
   SELECT F.isin,
          F.nombre,
          COALESCE(F.alias, F.nombre) AS alias,
          F.gestora,
          F.moneda,
          F.riesgo,
          F.scraper,
          COALESCE(S.data, F.isin) AS scraper_data,
          F.activo
   FROM tFondo F
      LEFT JOIN tScraperData S ON F.scraper = S.scraper AND F.isin = S.isin;


CREATE TRIGGER IF NOT EXISTS Fondo_BI INSTEAD OF INSERT ON Fondo
FOR EACH ROW
   BEGIN
      INSERT INTO tFondo VALUES (NEW.isin, NEW.nombre, NEW.alias, NEW.gestora, NEW.moneda, NEW.riesgo, NEW.scraper, NEW.activo);
      UPDATE Fondo SET (scraper, scraper_data) = (NEW.scraper, NEW.scraper_data) WHERE isin = NEW.isin;
   END;


--
-- La actualización de scraper o scraper_data de Fondos
-- permite actualizar tScraperData.
--
CREATE TRIGGER IF NOT EXISTS Fondo_BU INSTEAD OF UPDATE OF scraper, scraper_data ON Fondo
FOR EACH ROW
   BEGIN
      INSERT OR REPLACE INTO tScraperData
         SELECT NEW.scraper, NEW.isin, NEW.scraper_data WHERE NEW.scraper_data != OLD.scraper_data;
      -- Si fijamos a NULL el valor de scraper_data, borramos la entrada de tScraperData
      DELETE FROM tScraperData WHERE isin = NEW.isin AND scraper = NEW.scraper AND NEW.scraper_data IS NULL;
      UPDATE tFondo SET scraper = NEW.scraper WHERE isin = NEW.isin AND NEW.scraper != OLD.scraper;
   END;


--
-- CUENTA partícipe
--
CREATE TABLE IF NOT EXISTS tCuenta (
   cuentaID          VARCHAR(30)    PRIMARY KEY,
   isin              CHAR(12)       NOT NULL,
   comercializadora  VARCHAR(50)    NOT NULL,

   CONSTRAINT fk_cue_fon FOREIGN KEY(isin) REFERENCES tFondo(isin) ON DELETE SET NULL ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS cue_sin ON tCuenta(isin);

--
-- SUSCRIPCIÓN a fondos de inversión.
--   + Las que tengan origen, proceden de traspaso desde otro fondo;
--     las que no, son una nueva inversión.
--   + Si no se especifica coste de compra, se supone que no hay comisiones
--     de compra y, en consecuencia, este es iniciales*VL. Al insertar
--     un registro con coste nulo, se intenta escribir automáticamente este
--     valor, pero para ello es necesario poder obtener VL de la tabla Cotizacion.
--     Si no existe, el valor se dejará nulo, aunque un trigger de Cotización
--     le dará valor cuando se inserte el VL en tal tabla.
--   + La fecha efectiva de inversión es la fecha de suscripción, si el dinero
--     es nuevo, o la fecha efectiva de inversión de la suscripción de la que
--     procede el dinero, si la suscripción es fruto de un traspaso.
--
CREATE TABLE IF NOT EXISTS tSuscripcion (
   suscripcionID INTEGER          PRIMARY KEY AUTOINCREMENT,
   cuentaID      VARCHAR(30)      NOT NULL,   -- Cuenta partícipe
   fecha_i       DATE             NOT NULL,   -- Fecha efectiva de inversión (se hereda si procede de traspaso)
   fecha         DATE             NOT NULL,   -- Fecha de suscripcion,
   iniciales     DECIMAL(13, 5)   CHECK(iniciales>0),  -- Participaciones suscritas inicialmente
   coste         DECIMAL(10, 2),              -- Coste de la inversión.
   -- origen pospuesto hasta definir tVenta.

   CONSTRAINT check_suscr_def CHECK(iniciales IS NOT NULL OR coste IS NOT NULL),
   CONSTRAINT fk_sus_cue  FOREIGN KEY(cuentaID) REFERENCES  tCuenta(cuentaID) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Es la vista de lo que queda de cada Suscripcion:
-- + Las particiones restantes.
-- + La fecha de compra.
-- + El coste exclusivamente de las particiones restantes.
-- + Se ordena por fecha de inversión (ya que no aparece)
CREATE VIEW IF NOT EXISTS Suscripcion AS
SELECT S.suscripcionID,
       S.cuentaID,
       S.fecha,
       S.iniciales - COALESCE(SUM(V.participaciones), 0) AS participaciones,
       ROUND(S.coste*(1 - COALESCE(SUM(V.participaciones), 0)/S.iniciales), 2) AS coste,
       S.origen
FROM tSuscripcion S LEFT JOIN tVenta V USING(suscripcionID)
GROUP BY suscripcionID ORDER BY S.fecha_i;

CREATE TRIGGER IF NOT EXISTS Suscripcion_BI INSTEAD OF INSERT ON Suscripcion
FOR EACH ROW
   BEGIN
      INSERT INTO tSuscripcion
      -- Valor liquidativo de la participación el día de la suscripción
      -- Si no existe, se devuelve un valor NULO.
      WITH VL AS (
         SELECT Co.vl AS 'vl'
            FROM tCuenta Cu JOIN tCotizacion Co USING(isin)
            WHERE (NEW.participaciones IS NULL OR NEW.coste IS NULL) AND
                  Cu.cuentaID = NEW.cuentaID AND Co.fecha = NEW.fecha
         UNION SELECT NULL AS 'vl'
         ORDER BY vl DESC LIMIT 1
      )
      SELECT NEW.suscripcionID,
             NEW.cuentaID,
             -- La fecha de inversión se toma de la inversión precedente
             -- o, si no la hay, es la propia fecha de compra.
             CASE WHEN NEW.origen IS NULL
                THEN NEW.fecha
                ELSE (SELECT S.fecha_i
                      FROM Venta V JOIN tSuscripcion S USING(suscripcionID)
                      WHERE V.ventaID = NEW.origen)
             END,
             NEW.fecha,
             -- Si participaciones es NULO, se intenta calcular como coste/VL
             ROUND(COALESCE(NEW.participaciones, NEW.coste / vl) - 0.000005, 5),
             -- Si coste es NULO, vale participaciones*VL.
             ROUND(COALESCE(NEW.coste, vl * NEW.participaciones), 2),
             NEW.origen
      FROM VL;
   END;

CREATE TABLE IF NOT EXISTS tOrdenVenta (
   orden           INTEGER       PRIMARY KEY,
   fecha           DATE          NOT NULL,
   comentario      VARCHAR(100)
);

--
-- VENTA total o paricial de una suscripción.
--   + Si la venta es total, participaciones puede ser NULO.
--   + "reintegro" funciona como "coste" en Suscripcion: si es NULO,
--     se sobrentiende que no hay comisiones de venta y que el reintegro
--     es participaciones*VL. También Cotización tiene un trigger para
--     definir el reintegro en cuanto se conozca el VL.
--
CREATE TABLE IF NOT EXISTS tVenta (
   ventaID         INTEGER        PRIMARY KEY AUTOINCREMENT,
   orden           INTEGER        NOT NULL,
   suscripcionID   INTEGER        NOT NULL,
   participaciones DECIMAL(10, 2) CHECK(participaciones>0), -- Número de participaciones vendidas.
   reintegro       DECIMAL(10, 2) CHECK(reintegro>=0),  -- Dinero que se obtiene del reembolso.

   CONSTRAINT check_venta CHECK(participaciones IS NOT NULL OR reintegro IS NOT NULL),
   CONSTRAINT uniq_ord_suscr UNIQUE(orden, suscripcionID),
   CONSTRAINT fk_sus_ven FOREIGN KEY(suscripcionID) REFERENCES tSuscripcion(suscripcionID) ON DELETE SET NULL ON UPDATE CASCADE,
   CONSTRAINT fk_ord_ven FOREIGN KEY(orden) REFERENCES tOrdenVenta(orden) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS venta_ord ON tVenta(orden);
CREATE UNIQUE INDEX IF NOT EXISTS ven_uniq ON tVenta(orden, suscripcionID);

ALTER TABLE tSuscripcion ADD COLUMN 
   -- Si la suscripción es fruto de un traspaso, origen es la venta que la originó
   origen INTEGER REFERENCES tVenta(ventaID) ON DELETE SET NULL ON UPDATE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS sus_uniq ON tSuscripcion(origen, cuentaID, fecha);

CREATE VIEW IF NOT EXISTS Venta AS
SELECT
   V.ventaID,
   V.orden,
   V.suscripcionID,
   O.fecha,
   V.participaciones,
   V.reintegro,
   O.comentario
FROM tVenta V JOIN tOrdenVenta O USING(orden);

CREATE TRIGGER IF NOT EXISTS Venta_BI INSTEAD OF INSERT ON Venta
FOR EACH ROW
   BEGIN
      SELECT RAISE(FAIL, "Venta imposible: la suscripción asociada no tiene definidas participaciones")
      FROM Suscripcion WHERE suscripcionID = NEW.suscripcionID AND participaciones IS NULL;

      -- Sólo añade el registro de orden, si la orden no existe.
      INSERT INTO tOrdenVenta
         SELECT NEW.orden, NEW.fecha, NEW.comentario WHERE NOT EXISTS(SELECT orden FROM tOrdenVenta WHERE orden = NEW.orden);

      INSERT INTO tVenta VALUES (
          NEW.ventaID,
          COALESCE(NEW.orden, last_insert_rowid()),
          NEW.suscripcionID,
          -- Si participaciones es NULO, las venta es total.
          COALESCE(
            NEW.participaciones,
            (SELECT participaciones FROM Suscripcion WHERE suscripcionID = NEW.suscripcionID)
          ),
          NEW.reintegro
      );

      UPDATE Venta SET participaciones = participaciones,
                       -- Si reintegro es NULO, se calcula como participaciones*VL.
                       reintegro = COALESCE(reintegro, participaciones *
                                   (SELECT Co.vl
                                    FROM tSuscripcion S
                                      JOIN tCuenta Cu USING(cuentaID)
                                      JOIN tCotizacion Co USING(isin)
                                    WHERE S.suscripcionID = NEW.suscripcionID
                                          AND Co.fecha = NEW.fecha))
      WHERE ventaID = last_insert_rowid();
   END;

-- Truncamiento de los valores numéricos.
CREATE TRIGGER IF NOT EXISTS Venta_BU INSTEAD OF UPDATE OF participaciones, reintegro ON Venta
FOR EACH ROW
   BEGIN
      UPDATE tVenta SET participaciones = ROUND(NEW.participaciones - 0.000005, 5),
                        reintegro = ROUND(NEW.reintegro, 2)
      WHERE ventaID = NEW.ventaID;
   END;


--
-- COTIZACION en cada fecha.
--
CREATE TABLE IF NOT EXISTS tCotizacion (
   isin        CHAR(12)       NOT NULL,
   fecha       DATE           NOT NULL,     -- Fecha de la cotización
   vl          DECIMAL(5,4)   NOT NULL,     -- Valor liquidativo en esa fecha

   CONSTRAINT  pk_cot      PRIMARY KEY(isin, fecha),
   CONSTRAINT  fk_cot_fon  FOREIGN KEY(isin)  REFERENCES  tFondo(isin)   ON DELETE CASCADE ON UPDATE CASCADE
);

-- Al insertan un nuevo VL de un fondo, se buscan las suscripciones
-- y ventas de las que quedara pendiente el cálculo del coste y el reintegro.
CREATE TRIGGER IF NOT EXISTS tCotizacion_AI AFTER INSERT ON tCotizacion
FOR EACH ROW
BEGIN
   UPDATE tSuscripcion SET coste = COALESCE(coste, ROUND(NEW.vl*iniciales, 2)),
                           iniciales = COALESCE(iniciales, MAX(0, ROUND(coste/NEW.vl - 0.000005, 5)))
   WHERE (coste IS NULL OR iniciales IS NULL)
         AND fecha = NEW.fecha
         AND cuentaID IN (SELECT cuentaID FROM tCuenta WHERE isin = NEW.isin);

   UPDATE Venta SET reintegro = NEW.vl*participaciones
   WHERE reintegro IS NULL AND fecha = NEW.fecha
                           AND suscripcionID IN (SELECT S.suscripcionID
                                                 FROM tCuenta C JOIN tSuscripcion S USING(cuentaID)
                                                 WHERE C.isin = NEW.isin);
END;


--
-- TRASPASO: Venta + Suscripción posterior
--
CREATE VIEW IF NOT EXISTS Traspaso AS
   SELECT V.ventaID as traspasoID,
          V.orden AS orden,
          V.suscripcionID AS origen,
          V.fecha AS fecha_v,
          V.participaciones AS part_v,
          V.reintegro AS monto,
          S.cuentaID AS destino,
          S.fecha AS fecha_c,
          S.iniciales AS part_c,
          V.comentario
   FROM Venta V
      LEFT JOIN tSuscripcion S ON V.ventaID = S.origen;

CREATE TRIGGER IF NOT EXISTS Traspaso_BI INSTEAD OF INSERT ON Traspaso
FOR EACH ROW
   BEGIN
      SELECT RAISE(FAIL, "La fecha de compra no puede ser anterior a la de venta")
      WHERE NEW.fecha_c IS NOT NULL AND NEW.fecha_c < NEW.fecha_v;

      -- Operación de venta.
      INSERT INTO Venta (orden, suscripcionID, fecha, participaciones, reintegro, comentario)
         VALUES (NEW.orden, NEW.origen, NEW.fecha_v, NEW.part_v, NEW.monto, NEW.comentario);

      -- last_insert_rowid() no está definido, porque Venta es una VISTA, no una TABLA

      -- Operación de compra (sólo si se expresa cuenta de destino)
      INSERT INTO Suscripcion
         SELECT NULL,
                NEW.destino,
                COALESCE(NEW.fecha_c, V.fecha),
                CASE
                   WHEN NEW.part_c IS NOT NULL THEN NEW.part_c
                   -- Si no se definen particiones de compra ni el monto,
                   -- pero los fondos de origen y destino son los mismos
                   -- y las fechas de compra y de venta también lo son.
                   -- entonces se trata de un mero cambio de comercializadora.
                   WHEN NEW.monto IS NULL
                        AND COALESCE(NEW.fecha_c, V.fecha) = V.fecha
                        AND (SELECT COUNT(DISTINCT C.isin) = 1
                             FROM tSuscripcion S JOIN tCuenta C USING(cuentaID)
                             WHERE C.cuentaID = NEW.destino OR S.suscripcionID = V.suscripcionID) THEN participaciones
                   ELSE NULL
                END,
                V.reintegro,
                V.ventaID
         FROM Venta V
         WHERE NEW.destino IS NOT NULL AND V.orden = NEW.orden AND V.suscripcionID = NEW.origen;
   END;

--
-- RESUMEN de suscripciones por cuenta partícipe.
--
CREATE VIEW IF NOT EXISTS Cartera AS
   WITH prev AS (
      SELECT Cu.isin AS isin,
             Cu.cuentaID,
             Cu.comercializadora,
             SUM(S.coste) AS capital,
             Co.fecha,
             Co.vl,
             SUM(S.participaciones) AS participaciones
      FROM Suscripcion S
               LEFT JOIN
           tCuenta Cu ON Cu.cuentaID = S.cuentaID
               LEFT JOIN
           (SELECT isin, fecha, vl
                FROM tCotizacion
                GROUP BY isin HAVING fecha = MAX(fecha)
           ) Co ON Co.isin = Cu.isin 
      GROUP BY Cu.isin, Cu.comercializadora
   )
   SELECT isin,
          cuentaID,
          comercializadora,
          -- Como no se analiza la evolución, no se puede saber
          -- cuál es la ganancia o pérdida anterior a la inversión actual.
          0 as anterior,
          capital,
          participaciones,
          ROUND(vl*participaciones/SUM(vl*participaciones) OVER(), 4) AS peso,
          fecha,
          vl,
          ROUND(vl*participaciones, 2) AS valoracion,
          ROUND(1.0*vl*participaciones/capital - 1, 4) AS plusvalia
   FROM prev ORDER BY isin, comercializadora;

--
-- HISTORIAL de inversiones.
--    Desgrana el historial de cada rembolso definitivo o cada conjunto de participaciones
--    cuya inversión aún se conserva. En cada historial la tupla (orden, desinversion) es única.
--    La idea es hacer inicialmente una consulta que devuelva las suscripciones terinales,
--    y, tomadas estas, ir recursivamente obteniendo las suscripciones
--    que las originaron. Esto obtendrá un historial desde la inversión original hasta
--    la suscripción terminal.
--
--    Si la suscripción terminal:
--
--    a) se vendió, se proporciona el rembolso obtenido por la venta y las participaciones que
--       se vendieron. No se proporciona el valor liquidativo.
--    b) no se vendió, entonces se estima el rembolso tomando como valor liquidativo el último disponible
--       en la base de datos. EN este caso sí se proporciona el valor liquidativo. Las participaciones tomadas
--       son las qye resten en la suscripción.
--
--    WITH recursive: https://blog.expensify.com/2015/09/25/the-simplest-sqlite-common-table-expression-tutorial/
--
CREATE VIEW IF NOT EXISTS Historial AS
   WITH RECURSIVE Origen AS (
      -- Suscripciones en las que aún se tiene invertido dinero.
      SELECT S.suscripcionID as desinversion,
             S.suscripcionID,
             S.cuentaID,
             0 as orden,  -- Se conservan las partic, por lo que no hay orden  de venta (0)
             1.0*Sp.participaciones/S.iniciales AS parcial,
             S.fecha_i,
             S.fecha,
             Co.fecha AS fecha_v,
             Sp.coste,
             Co.vl,
             Sp.participaciones AS participaciones,
             ROUND(Co.vl*Sp.participaciones, 2) AS reintegro,
             S.origen
      FROM tSuscripcion S
         JOIN Suscripcion Sp USING(suscripcionID)
         JOIN tCuenta Cu USING(cuentaID)
         LEFT JOIN (SELECT * FROM tCotizacion  -- La última cotización registrada.
                    GROUP BY isin HAVING fecha = MAX(fecha)) Co USING(isin)
      WHERE Sp.participaciones > 0
         UNION ALL
      -- Parte ya rembolsada de las suscripciones
      SELECT S.suscripcionID as desinversion,
             S.suscripcionID,
             S.cuentaID,
             V.orden AS orden,
             1.0*V.participaciones/S.iniciales AS parcial,
             S.fecha_i,
             S.fecha,
             V.fecha AS fecha_v,
             ROUND(1.0*S.coste*V.participaciones/S.iniciales, 2) AS coste,
             0 as vl,  -- 0 para evitar confusiones (podría calcularse con el VL de la fecha de venta)
             V.participaciones,
             V.reintegro AS reintegro,
             S.origen
      FROM Venta V
         JOIN tSuscripcion S USING(suscripcionID)
         LEFT JOIN tSuscripcion Sv ON Sv.origen = V.ventaID 
      WHERE Sv.origen IS NULL
         UNION ALL
      SELECT O.desinversion,
             S.suscripcionID,
             S.cuentaID,
             O.orden,  -- 0, si las participaciones siguen invertidas
             O.parcial*V.participaciones/S.iniciales as parcial,
             S.fecha_i,
             S.fecha,
             V.fecha as fecha_v,
             ROUND(S.coste*V.participaciones/S.iniciales*O.parcial, 2) AS coste,
             NULL as vl, -- Es irrelevante cuánto valía entonces el VL, así que evitamos calcularlo.
             ROUND(V.participaciones*O.parcial, 4) AS participaciones,
             ROUND(V.reintegro*O.parcial, 2) AS reintegro,
             S.origen
      FROM Venta V
         JOIN tSuscripcion S USING(suscripcionID)
         JOIN Origen O ON O.origen = V.ventaID 
   )
   SELECT desinversion,
          suscripcionID,
          first_value(suscripcionID) OVER (PARTITION BY desinversion,orden ORDER BY fecha) AS origen,
          cuentaID,
          orden,
          fecha_i,
          fecha,
          fecha_v,
          coste,
          first_value(coste) OVER (PARTITION BY desinversion,orden ORDER BY fecha) AS capital,
          vl,
          participaciones,
          reintegro
   FROM Origen
   ORDER BY orden, desinversion, fecha;


-- Permite manejar las ventas usando como origen
-- la cuenta partícipe en vez de las suscripciones individuales.
CREATE VIEW IF NOT EXISTS VentaAggr AS
   SELECT V.orden,
          S.cuentaID,
          V.fecha,
          -- Número de suscripciones distintas que se venden.
          -- Siempre es un número entero por lo que si se vendió
          -- una suscripción completa y la cuarta parte de otra,
          -- el campo valdrá 2.
          -- Su utilidad está en la inserción de registros. Por ejemplo,
          -- si se proporciona un 2, esto significa que se venden las participaciones
          -- correspondientes a las dos más antiguas suscripciones al fondo.
          COUNT(*) AS suscripciones,
          SUM(V.participaciones) AS participaciones,
          SUM(V.reintegro) AS reintegro,
          V.comentario
   FROM Venta V
         JOIN
        tSuscripcion S USING(suscripcionID)
   GROUP BY V.orden;

CREATE TRIGGER IF NOT EXISTS VentaAggr_BI INSTEAD OF INSERT ON VentaAggr
FOR EACH ROW
   BEGIN
      INSERT INTO TraspasoAggr VALUES
          (NEW.orden, NEW.cuentaID, NEW.fecha, NEW.suscripciones, NEW.participaciones, NEW.reintegro, NULL, NULL, NULL, NEW.comentario);
   END;

-- Permite manejar los traspasos usando como origen
-- la cuenta partícipe en vez de suscripciones a esa cuenta.
CREATE VIEW IF NOT EXISTS TraspasoAggr AS
   SELECT T.orden,
          S.cuentaID as origen,
          T.fecha_v,
          COUNT(*) AS suscripciones,  -- Ver VentaAggr
          SUM(T.part_v) AS part_v,
          SUM(T.monto) AS monto,
          T.destino,
          T.fecha_c,
          SUM(T.part_c) AS part_c,
          T.comentario
   FROM Traspaso T
         JOIN
        tSuscripcion S ON S.suscripcionID = T.origen
   GROUP BY T.orden;

-- Convierte el traspaso agregado (cuyo origen es una cuenta).
-- en un conjunto de traspasos simples (cuyos orígenes son suscripciones)
CREATE TRIGGER IF NOT EXISTS TraspasoAggr_II1 INSTEAD OF INSERT ON TraspasoAggr
FOR EACH ROW
   WHEN NEW.suscripciones IS NOT NULL
   BEGIN
      SELECT RAISE(FAIL, "No puede especificar a la vez número de suscripciones y de participaciones")
      WHERE NEW.part_v IS NOT NULL;

      SELECT RAISE(FAIL, "Demasiadas suscripciones")
      FROM Suscripcion WHERE cuentaID = NEW.origen AND participaciones > 0
      GROUP BY cuentaID
      HAVING COUNT(*) < NEW.suscripciones;

      -- Debemos generar el número de orden
      INSERT INTO tOrdenVenta VALUES (NEW.orden, NEW.fecha_v, NEW.comentario);

      INSERT INTO Traspaso
      WITH SuscripcionVendida AS
         (SELECT * FROM Suscripcion 
         WHERE cuentaID = NEW.origen AND participaciones > 0
         LIMIT NEW.suscripciones)
      SELECT NULL,
             last_insert_rowid(),
             suscripcionID,
             NEW.fecha_v,
             participaciones,
             NEW.monto*participaciones/SUM(participaciones) OVER (),
             NEW.destino,
             NEW.fecha_c,
             NEW.part_c*participaciones/SUM(participaciones) OVER (),
             NEW.comentario
      FROM SuscripcionVendida;
   END;

CREATE TRIGGER IF NOT EXISTS TraspasoAggr_II2 INSTEAD OF INSERT ON TraspasoAggr
FOR EACH ROW
   WHEN NEW.suscripciones IS NULL
   BEGIN
      SELECT RAISE(FAIL, "Demasiadas participaciones")
      FROM Suscripcion WHERE cuentaID = NEW.origen
      GROUP BY cuentaID HAVING SUM(participaciones) < NEW.part_v;

      -- Debemos generar el número de orden
      INSERT INTO tOrdenVenta VALUES (NEW.orden, NEW.fecha_v, NEW.comentario);

      INSERT INTO Traspaso
      WITH SuscripcionAggr AS
           -- Desglosa cada suscripción añadiendo una columna
           -- que contiene la particiones acomuladas en la cuenta.
           -- Por ejemplo, si se han hecho dos compras de 100 y
           -- 250 participaciones para una cuenta partícipe, la columna
           -- adicional contendrá para la primera compra 100 y para la
           -- segunda, 350.
              (SELECT S.suscripcionID,
                      S.cuentaID,
                      S.fecha_i,
                      Sp.participaciones AS part,
                      SUM(Sp.participaciones) OVER (PARTITION BY S.cuentaID ORDER BY S.fecha_i) AS partacc,
                      Sp.participaciones / SUM(Sp.participaciones) OVER (PARTITION BY S.cuentaID) AS porcpart
               FROM tSuscripcion S
                      JOIN Suscripcion Sp USING(suscripcionID)
               WHERE Sp.participaciones > 0
               ORDER BY S.cuentaID, S.fecha_i),
           VentaDesagregada AS
              (SELECT suscripcionID,
                      NEW.fecha_v AS fecha,
                      CASE WHEN NEW.part_v IS NULL THEN part
                           WHEN ROUND(NEW.part_v - partacc, 5) >= 0 THEN part
                           WHEN ROUND(partacc - part - NEW.part_v, 5) > 0 THEN 0
                           ELSE ROUND(NEW.part_v - partacc + part, 5)
                      END AS vendidas,
                      CASE WHEN NEW.part_v IS NULL THEN NEW.monto*porcpart
                           WHEN ROUND(NEW.part_v - partacc, 5) >= 0 THEN NEW.monto*part/NEW.part_v
                           WHEN ROUND(partacc - part - NEW.part_v, 5) > 0 THEN 0
                           ELSE ROUND(NEW.monto*(NEW.part_v - partacc + part)/NEW.part_v, 2)
                      END AS reintegro
               FROM SuscripcionAggr
               WHERE cuentaID = NEW.origen AND part > 0 AND vendidas > 0),
           NuevaOrden AS (SELECT last_insert_rowid())
      SELECT NULL, *, NEW.destino, NEW.fecha_c, NEW.part_c*vendidas/SUM(vendidas) OVER (), NEW.comentario
      FROM NuevaOrden, VentaDesagregada;
   END;


-- Genera la evolución temporal de la rentabilidad de cada suscripción
-- (las que se listan en la vista Plusvalía) en periodos de "meses"
-- o "semanas" entre dos fechas de tiempo. No especificar la fecha inicial
-- implica desde que se hizo la primera inversión; y no especificar
-- la final, hasta el último día registrado. La salisd es útil para generar un
-- gráfico con la evolución de las rentabilidades.
--
-- Para usar la vista deben definirse las fechas iniciales,
-- finales y el periodo.
--
-- WITH Tiempo(inicial, final, periodo) AS (SELECT NULL, NULL, 'semanas'),
-- SELECT * FROM Evolucion;
--
CREATE VIEW IF NOT EXISTS Evolucion AS
   -- Extrae las cotizaciones en periodo de meses o semanas:
   -- Toma la fecha de inversión más antigua, genera fechas
   -- separadas el periodo establecido y obtiene las cotizaciones
   -- para cada una de esas fechas o, si no existe, la anteriormente
   -- más cercana.
   WITH RECURSIVE
      Comienzo(fecha) AS (
         SELECT CASE
                  WHEN (SELECT inicial FROM Tiempo) >= (SELECT MIN(fecha) FROM tSuscripcion) THEN (SELECT inicial FROM Tiempo)
                  ELSE (SELECT MIN(fecha) FROM tSuscripcion)
                END
      ),
      Fin(fecha) AS (
         SELECT CASE
                  WHEN (SELECT final FROM Tiempo) <= DATE("now") THEN (SELECT final FROM Tiempo)
                  ELSE DATE("now")
                END
      ),
      Periodos(periodo, delta) AS (SELECT "semanas", "7 days" UNION SELECT "meses", "1 month"),
      RangoInicial(periodo, delta, inf, sup) AS (
         SELECT P.*, DATE(S.fecha, "-" || P.delta), S.fecha
         FROM (SELECT fecha FROM Comienzo) S, Periodos P
         WHERE P.periodo = (SELECT periodo FROM Tiempo)
      ),
      Secuencia AS (
         SELECT * FROM RangoInicial
            UNION ALL
         SELECT periodo, delta, sup AS inf, DATE(sup, "+" || delta) AS sup FROM Secuencia WHERE sup <= (SELECT * FROM Fin)
      ),
      Temporizacion AS (
         SELECT S.periodo, C.isin, C.fecha, C.vl
         FROM tCotizacion C JOIN Secuencia S ON C.fecha > S.inf AND C.fecha <= S.sup
         GROUP BY C.isin, S.periodo, S.inf, S.sup HAVING C.fecha = MAX(C.fecha)
         ORDER BY C.isin, C.fecha
      ),
      Puntos AS (
         -- El coste inicial de la inversión puede quedar excluido
         -- si la fecha de compra, no está incluida en la temporización.
         -- así que para subsanarlo hacemos esta consulta. Ahora bien,
         -- la operación inicial es relevante sólo si su fecha no es
         -- anterior a la FechaInicial.
         SELECT (SELECT periodo FROM Tiempo) AS periodo,
                H.desinversion,
                H.suscripcionID,
                H.orden,
                H.fecha AS fecha_c,
                H.fecha_v,
                H.participaciones,
                C.isin,
                H.fecha AS fecha,
                H.coste AS rembolso
         FROM Historial H JOIN tCuenta C USING(cuentaID)
            LEFT JOIN Temporizacion T USING(isin, fecha)
         WHERE H.fecha_i = H.fecha AND T.fecha IS NULL
                  AND H.fecha >= (SELECT * FROM Comienzo)
            UNION ALL
         -- Interpolamos el Historial de cada inversión
         -- para obtener las valoraciones intermedias.
         SELECT T.periodo,
                H.desinversion,
                H.suscripcionID,
                H.orden,
                H.fecha AS fecha_c,
                H.fecha_v,
                H.participaciones,
                T.isin,
                T.fecha AS fecha,
                H.participaciones*T.vl AS rembolso
         FROM Historial H
            JOIN tCuenta C USING(cuentaID)
            LEFT JOIN Temporizacion T ON C.isin = T.isin
         WHERE T.fecha >= H.fecha AND T.fecha <= H.fecha_v
      ),
      -- Puntos de inicio de la inversión
      PuntosIniciales AS (
         SELECT *
         FROM Puntos
         GROUP BY desinversion, orden
         HAVING fecha = MIN(fecha)
      )
   -- La evolución propiamente
   SELECT P.periodo,
          P.desinversion,
          P.suscripcionID,
          P.orden,
          I.rembolso AS coste,
          P.fecha_c,
          P.fecha_v,
          P.participaciones,
          P.isin,
          P.fecha,
          P.rembolso
   FROM Puntos P JOIN PuntosIniciales I USING(desinversion, orden)
      UNION ALL
   -- Registros que definen cuál ha sido la temporización de fechas.
   SELECT periodo, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, sup AS fecha, NULL
   FROM Secuencia
   ORDER BY desinversion, orden, fecha;


---
--- CarteraHistorica permite consultar la cartera como hace Cartera.
--  pero estableciendo fecha inicial o final de referencia o tomando ambas.
--  Esto quiere decir que si se establece una fecha inicial, en vez de tomar
--  el capital inicial de inversión, se toma la valoración de las inversiones
--  en esa fecha. Establecer una fecha final, valora la inversión en tal fecha
--  en vez de en la última disponible.
--
--  Para usar la vista, forzosamente, hay que indicar cuáles son las fechas
--  mediante CTE (déjese a NULL para no indicarla):
--
--  WITH Tiempo(inicial, final) AS (SELECT '2020-05-01', NULL),
--  SELECT * FROM CarteraHistorica;
--
--  Obviamente dejar a NULL ambas fechas, equivale consultar Cartera)
---
CREATE VIEW IF NOT EXISTS CarteraHistorica AS
   WITH
      -- Suscripciones que en realidad son un mero cambio de comercializadora
      CambioComercializadora AS (
         SELECT S2.suscripcionID
         FROM tSuscripcion S1
                 JOIN
              tVenta V USING(suscripcionID)
                 JOIN
              tSuscripcion S2 ON S2.origen = V.ventaID
                 JOIN
              tCuenta T1 ON T1.cuentaID = S1.cuentaID
                 JOIN
              tCuenta T2 ON T2.cuentaID = S2.cuentaID
         WHERE T1.isin = T2.isin
            AND S2.fecha >= COALESCE((SELECT inicial FROM Tiempo), '')
      ),
      Progreso AS (
         SELECT H.desinversion,
                H.suscripcionID,
                H.orden,
                H.fecha AS fecha_c,
                H.fecha_v,
                H.participaciones,
                C.cuentaID,
                Co.fecha,
                Co.vl,
                H.coste
         FROM Historial H
            JOIN tCuenta C USING(cuentaID)
            LEFT JOIN tCotizacion Co USING(isin)
         WHERE orden = 0 -- Solo consideramos inversiones que no se han vendido.
            AND H.fecha <= Co.fecha AND H.fecha_v >= Co.fecha
            AND Co.fecha >= COALESCE((SELECT inicial FROM Tiempo), '')
            AND Co.fecha <= COALESCE((SELECT final FROM Tiempo), '9999-99-99')
      ),
      InversionOriginal AS (
         SELECT * FROM Progreso
         GROUP BY desinversion, orden
         HAVING fecha = MIN(fecha)
      ),
      UltimaInversion AS (
         SELECT * FROM Progreso
         GROUP BY desinversion, orden
         HAVING fecha = MAX(fecha)
      ),
      -- Para el coste debemos tener en cuenta que si la inversión
      -- es en realidad un cambio de comercializadora, no vale.
      UltimaInversionReal AS (
         SELECT * FROM Progreso
         WHERE suscripcionID NOT IN (SELECT * FROM CambioComercializadora)
         GROUP BY desinversion, orden
         HAVING fecha = MAX(fecha)
      ),
      Prev AS (
         SELECT P2.cuentaID,
                SUM(
                  CASE 
                     -- Si la compra se realizó dentro del periodo (FechaInicial, FechaFinal)
                     -- entonces usamos el coste de compra.
                     WHEN P1.fecha_c < (SELECT inicial FROM Tiempo) THEN P1.participaciones*P1.vl
                     ELSE P1.coste
                  END
                ) AS inicial,  -- Valoración a FechaInicial.
                SUM(
                  CASE
                     -- Si la inversión actual se hizo antes de FechaInicial
                     -- se debe tomar la valoración a FechaInicial.
                     WHEN P3.fecha_c < (SELECT inicial FROM Tiempo) THEN P1.participaciones*P1.vl
                     ELSE P3.coste
                  END
               ) AS capital,  -- Coste de compra de las participaciones actuales.
                P2.fecha,
                P2.vl,
                SUM(P2.participaciones) AS participaciones
         FROM InversionOriginal P1 
                 JOIN 
              UltimaInversion P2 USING(desinversion, orden)
                 JOIN
              UltimaInversionReal P3 USING(desinversion, orden)
         GROUP BY P2.cuentaID
      )
   SELECT isin,
          cuentaID,
          comercializadora,
          ROUND(capital - inicial, 2) AS anterior, -- Beneficio acomulado antes de la inversión vigente.
          capital, 
          participaciones,
          ROUND(vl*participaciones/SUM(vl*participaciones) OVER (), 4) AS peso,
          fecha,
          vl,
          ROUND(vl*participaciones, 2) AS valoracion,
          ROUND(1.0*vl*participaciones/capital - 1, 4) AS plusvalia
   FROM Prev JOIN tCuenta USING(cuentaID)
   ORDER BY isin, comercializadora;


-- Tabla para guardar los parámetros necesarios
-- para las vistas Evolucion y CarteraHistorica,
-- puesto que desde abril de 2021 no vale utilizar una CTE:
--
--  WITH Tiempo(inicial, final) AS (SELECT '2020-05-01', NULL),
--  SELECT * FROM CarteraHistorica;
--
CREATE TABLE IF NOT EXISTS Tiempo (
   inicial  DATE,
   final    DATE,
   periodo  CHAR(7) DEFAULT 'semanas'
);
INSERT OR REPLACE INTO Tiempo(inicial, final) VALUES (NULL, NULL);


-- Últimas Variaciones en las cotizaciones de los fondos activos:
--
--    SELECT * FROM VarCotizacion
--    WHERE numvar <= 1;
--
-- donde numvar es el número de variaciones que queremos ver.
-- En el ejemplo sólo se mostrará la última variación.
-- Obsérvese que hay que indicar '<=', no '='
--
CREATE VIEW IF NOT EXISTS VarCotizacion AS
   WITH FondoActivo AS (
            SELECT DISTINCT C.isin
            FROM tCuenta C
                  JOIN
                 Suscripcion S USING(cuentaID)
            WHERE S.participaciones > 0
        ),
        Cotizacion AS (
            SELECT *, RANK() OVER (
                        PARTITION BY isin
                        ORDER BY fecha DESC
                      ) AS orden
            FROM tCotizacion JOIN FondoActivo USING(isin)
        )
   SELECT C1.isin,
          C1.fecha,
          C1.vl,
          ROUND(C1.vl/C2.vl - 1, 4)*100 AS variacion,
          C1.orden AS numvar
   FROM Cotizacion C1 JOIN Cotizacion C2 ON C1.isin = C2.isin AND C1.orden = C2.orden - 1
