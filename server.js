// server.js
const express = require('express');
const cors = require('cors');
const duckdb = require('duckdb');

const app = express();
app.use(cors());

// Ruta a tu base de datos DuckDB
const DB_PATH = '/Users/ronydelcid/Desktop/censo_copy.db';


// Conexión a DuckDB
const db = new duckdb.Database(DB_PATH);
const conn = db.connect();

// Sanitiza: solo dígitos para la identidad
const onlyDigits = s => (s || '').replace(/[^\d]/g, '');

app.get('/buscar', (req, res) => {
  try {
    const identidad = onlyDigits(req.query.identidad || '');
    if (!identidad || identidad.length < 6) {
      return res.status(400).json({ error: 'Parámetro "identidad" inválido' });
    }

    const sql = `
      SELECT 
        NUMERO_IDENTIDAD,
        PRIMER_NOMBRE, SEGUNDO_NOMBRE,
        PRIMER_APELLIDO, SEGUNDO_APELLIDO,
        NOMBRE_DEPARTAMENTO, NOMBRE_MUNICIPIO, NOMBRE_CENTRO,
        NUMERO_JRV, NUMERO_LINEA
      FROM censo
      WHERE NUMERO_IDENTIDAD = ?
      LIMIT 1;
    `;

    conn.all(sql, [identidad], (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });
      if (!rows || rows.length === 0) return res.json(null);
      res.json(rows[0]);
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API censo escuchando en http://localhost:${PORT}`));
