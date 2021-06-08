const knex = require('knex');

class TableColumn {
  constructor(initial) {
    this.name = null;
    this.type = null;
    this.length = null;
    this.position = null;
    this.nullable = false;
    this.isprimary = false;
    this.default = null;
    if (initial) {
      for (var k in initial) {
        this[k] = initial[k];
      }
    }
  }
}
/**
 * 
 * @param {knex} knex 
 * @param {String} tableName 
 * @param {String} schemaName 
 * @returns 
 */
async function getColumns(knex, tableName, schemaName = 'dbo') {
  const client = knex.context.client.config.client.toLowerCase();
  if (client === 'mssql' || client === 'mysql') {
    var columns = await knex("INFORMATION_SCHEMA.COLUMNS").where({
      'TABLE_SCHEMA': schemaName,
      'TABLE_NAME': tableName
    }).then(results => results.map(item => new TableColumn({
      name: item['COLUMN_NAME'],
      type: item['DATA_TYPE'],
      length: item['CHARACTER_MAXIMUM_LENGTH'],
      position: item['ORDINAL_POSITION'],
      nullable: item['IS_NULLABLE'] === 'YES',
      default: item['COLUMN_DEFAULT']
    }))).catch(console.error);
    var pkeys = await knex("INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as cu")
      .join("INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc", function () {
        this.on('tc.TABLE_CATALOG', '=', 'cu.TABLE_CATALOG')
          .on('tc.TABLE_NAME', '=', 'cu.TABLE_NAME')
          .on('tc.TABLE_SCHEMA', '=', 'cu.TABLE_SCHEMA')
          .on('tc.CONSTRAINT_TYPE', '=', knex.raw('?', ['PRIMARY KEY']));
      }).where('cu.TABLE_NAME', tableName).where('cu.TABLE_SCHEMA', schemaName).catch(console.error);
    var pkey = pkeys[0];
    if (pkey && pkey.COLUMN_NAME) {
      columns = columns.map(item => {
        if (item.name === pkey.COLUMN_NAME) {
          item.isprimary = true;
        }
        return item;
      })
    }
    return columns.sort((a, b) => Math.sign(a.position - b.position));
  }
  return [];
}

module.exports = {
  getColumns: getColumns
};
