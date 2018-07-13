const ctl = require('ctl');
const MySQL2 = require('mysql2/promise');
const log = ctl.library('logging')('mysql');
const config = ctl.library('config');

let pool;

const { port = 3306 } = config.mysql;
const options = Object.assign({}, {
  host: 'localhost',
  database: 'db',
  port: Number(port),
  connectionLimit: 20,
  connectTimeout: 1000, // return an error after 1s if no connection
}, config.mysql);

async function query(sql, args) {
  const conn = await pool.getConnection();
  let result = null;
  let error = null;
  try {
    result = await conn.query(sql, args);
  } catch (e) {
    error = e;
  }
  conn.release();
  if (error) {
    log.error(error);
    throw error;
  }
  return result;
}

const META_KEY = 'versions';
const META_TABLE = 'metainfo';

async function hasTable() {
  const { rowCount } = await query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE  table_schema = $1::text
    AND    table_name = $2::text;
  `, ['public', META_TABLE]);
  return (rowCount > 0);
}

exports.connect = async () => {
  log.info('Connect DB (mysql://%s:%s):', options.host, options.port, options.database);
  pool = MySQL2.createPool(options);
};

exports.pool = () => pool;
exports.query = query;

ctl.connect(exports.connect);
ctl.metainfo(async () => {
  if (!pool) return;
  return {
    set: async (val) => {
      await query(`
        INSERT INTO ${META_TABLE} (setting, value)
        VALUES ('${META_KEY}', ?)
        ON DUPLICATE KEY IGNORE
      `, [JSON.stringify(val)]);
      await query(`
        UPDATE ${META_TABLE}
        SET value = ?
        WHERE setting = '${META_KEY}'
      `, [JSON.stringify(val)]);
    },
    get: async () => {
      if (!await hasTable()) {
        await query(`
          CREATE TABLE IF NOT EXISTS ${META_TABLE} (setting text unique, value text)
        `);
      }
      const { rowCount, rows } = await query(`
        SELECT value FROM ${META_TABLE} WHERE setting = '${META_KEY}'
      `);
      if (rowCount === 0) return;
      return JSON.parse(rows[0].value);
    },
  };
});
