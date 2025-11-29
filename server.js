// server.js (CommonJS)
const express = require('express');
const cors = require('cors');
const duckdb = require('duckdb');
const fs = require('fs');
const https = require('https');

const app = express();
app.use(cors());

// URL pública de tu Parquet en Cloudflare R2
const PARQUET_URL = 'https://pub-9487f99c65424e2a8ed71289fa945c19.r2.dev/censo.parquet';
// En Render, /tmp es el lugar correcto para escribir archivos
const LOCAL_FILE = '/tmp/censo.parquet';

// Descarga el Parquet si no existe en /tmp
async function ensureLocalFile() {
  if (fs.existsSync(LOCAL_FILE)) return;
  console.log('Descargando Parquet desde R2...');
  await new Promise((resolve, reject) => {
    const file = fs.createWriteStream(LOCAL_FILE);
    https.get(PARQUET_URL, (res) => {
      if (res.statusCode !== 200) {
        return reject(new Error(`Descarga falló con status ${res.statusCode}`));
      }
      res.pipe(file);
      file.on('finish', () => file.close(resolve));
    }).on('error', reject);
  });
  console.log('Parquet descargado en /tmp.');
}

// DuckDB en memoria (no necesitamos archivo .db)
const db = new duckdb.Database(':memory:');
const conn = db.connect();

// Sanitiza: solo dígitos
const onlyDigits = (s) => (s || '').replace(/[^\d]/g, '');

// Healthcheck para Render
app.get('/', (_req, res) => res.send('OK'));

app.get('/buscar', async (req, res) => {
  try {
    const identidad = onlyDigits(req.query.identidad || '');
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
      res.json(rows && rows[0] ? rows[0] : null);
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () =>
  console.log(`✅ API del Censo lista en http://localhost:${PORT}`)
);
