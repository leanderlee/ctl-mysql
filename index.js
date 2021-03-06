const ctl = require('ctl');
const MySQL2 = require('mysql2');
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
  return new Promise((resolve, reject) => {
    pool.getConnection((getConnErr, conn) => {
      if (getConnErr) return reject(getConnErr);
      conn.query(sql, args, (queryErr, results, fields) => {
        pool.releaseConnection(conn);
        if (queryErr) {
          log.error(queryErr);
          return reject(queryErr);
        }
        resolve([results, fields]);
      });
    });
  });
}

const META_KEY = 'versions';
const META_TABLE = 'metainfo';

async function hasTable() {
  const [rows] = await query(`
    SHOW TABLES LIKE ?
  `, [META_TABLE]);
  return (rows.length > 0);
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
      const v = JSON.stringify(val);
      await query(`
        INSERT INTO ${META_TABLE} (setting, value)
        VALUES ('${META_KEY}', ?)
        ON DUPLICATE KEY UPDATE value = ?
      `, [v, v]);
      await query(`
        UPDATE ${META_TABLE}
        SET value = ?
        WHERE setting = '${META_KEY}'
      `, [JSON.stringify(val)]);
    },
    get: async () => {
      if (!await hasTable()) {
        await query(`
          CREATE TABLE IF NOT EXISTS ${META_TABLE} (setting varchar(256) unique, value text)
        `);
      }
      const [rows] = await query(`
        SELECT value FROM ${META_TABLE} WHERE setting = '${META_KEY}'
      `);
      if (rows.length === 0) return;
      return JSON.parse(rows[0].value);
    },
  };
});
