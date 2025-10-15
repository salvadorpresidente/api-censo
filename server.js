// server.js
import express from "express";
import cors from "cors";
import duckdb from "duckdb";
import fs from "fs";
import https from "https";

// Configuración
const app = express();
app.use(cors());

const PARQUET_URL = "https://pub-9487f99c65424e2a8ed71289fa945c19.r2.dev/censo.parquet";
const LOCAL_FILE = "/tmp/censo.parquet";

// Descargar el archivo Parquet desde Cloudflare si no existe localmente
async function ensureLocalFile() {
  if (fs.existsSync(LOCAL_FILE)) return;
  console.log("Descargando archivo Parquet desde Cloudflare R2...");
  const file = fs.createWriteStream(LOCAL_FILE);
  await new Promise((resolve, reject) => {
    https.get(PARQUET_URL, response => {
      if (response.statusCode !== 200) return reject(new Error(`Error ${response.statusCode}`));
      response.pipe(file);
      file.on("finish", () => file.close(resolve));
    }).on("error", reject);
  });
  console.log("Archivo descargado y guardado localmente.");
}

// Inicializar DuckDB
const db = new duckdb.Database(":memory:");
const conn = db.connect();

// Sanitiza el parámetro identidad
const onlyDigits = s => (s || "").replace(/[^\d]/g, "");

app.get("/buscar", async (req, res) => {
  try {
    const identidad = onlyDigits(req.query.identidad || "");
    if (!identidad || identidad.length < 6) {
      return res.status(400).json({ error: 'Parámetro "identidad" inválido' });
    }

    await ensureLocalFile();

    const sql = `
      SELECT
        NUMERO_IDENTIDAD,
        PRIMER_NOMBRE, SEGUNDO_NOMBRE,
        PRIMER_APELLIDO, SEGUNDO_APELLIDO,
        NOMBRE_DEPARTAMENTO, NOMBRE_MUNICIPIO, NOMBRE_CENTRO,
        NUMERO_JRV, NUMERO_LINEA
      FROM read_parquet('${LOCAL_FILE}')
      WHERE NUMERO_IDENTIDAD = '${identidad}'
      LIMIT 1;
    `;

    conn.all(sql, (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(rows?.[0] || null);
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ API del Censo lista en http://localhost:${PORT}`));

