const knex = require('knex');

class StoredProcedure {
  /**
     * @param {StoredProcedure} initial
     */
  constructor(initial) {
    /**
     * @type {String}
     */
    this.name = null;
    /**
     * @type {String}
     */
    this.schema = null;
    /**
     * @type {String}
     */
    this.db = null;
    /**
     * @type {String}
     */
    this.type = null;
    /**
     * @type {String}
     */
    this.returnType = null;
    /**
     * @type {String}
     */
    this.body = null;
    /**
     * @type {String}
     */
    this.bodyType = null;
    /**
     * @type {String}
     */
    this.accessMode = null;
    /**
     * @type {StoredProcedureParameter[]}
     */
    this.parameters = null;

    if (initial) {
      for (var k in initial) {
        this[k] = initial[k];
      }
    }
  }
}
class StoredProcedureParameter {
  /**
   * 
   * @param {StoredProcedureParameter} initial 
   */
  constructor(initial) {
    /**
     * @type {String}
     */
    this.name = null;
    /**
     * @type {String}
     */
    this.mode = null;
    /**
     * @type {Boolean}
     */
    this.isResult = null;
    /**
     * @type {Number}
     */
    this.position = null;
    /**
     * @type {String}
     */
    this.type = null;
    /**
     * @type {Number}
     */
    this.length = null;
    /**
     * @type {Number}
     */
    this.octetLength = null;
    if (initial) {
      for (var k in initial) {
        this[k] = initial[k];
      }
    }
  }
}
class Table {
  /**
   * 
   * @param {Table} initial 
   */
  constructor(initial) {
    /**
     * @type {String}
     */
    this.name = null;
    /**
     * @type {String}
     */
    this.type = null;
    /**
     * @type {String}
     */
    this.db = null;
    /**
     * @type {String}
     */
    this.schema = null;
    if (initial) {
      for (var k in initial) {
        this[k] = initial[k];
      }
    }
  }
}
class TableColumn {
  /**
   * 
   * @param {TableColumn} initial 
   */
  constructor(initial) {
    /**
     * @type {String}
     */
    this.name = null;
    /**
     * @type {String}
     */
    this.type = null;
    /**
     * @type {Number}
     */
    this.length = null;
    /**
     * @type {Number}
     */
    this.position = null;
    /**
     * @type {Boolean}
     */
    this.nullable = false;
    /**
     * @type {Boolean}
     */
    this.isprimary = false;
    /**
     * @type {Boolean}
     */
     this.isforeign = false;
     /**
     * @type {String}
     */
    this.foreignTable = false;
    /**
     * @type {String}
     */
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
 * @returns {TableColumn[]}
 */
async function getColumns(knex, tableName, schemaName = 'dbo') {
  const client = knex.context.client.config.client.toLowerCase();
  if (client === 'mssql' || client === 'mysql') {
    /**
     * @type {TableColumn[]}
     */
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
    var keys = await knex("INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as cu")
      .join("INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc", function () {
        /**
         * @type {knex.Knex.JoinClause}
         */
        var db = this;
        db.on('tc.TABLE_CATALOG', '=', 'cu.TABLE_CATALOG')
          .on('tc.TABLE_NAME', '=', 'cu.TABLE_NAME')
          .on('tc.TABLE_SCHEMA', '=', 'cu.TABLE_SCHEMA')
          .onIn('tc.CONSTRAINT_TYPE', ['PRIMARY KEY', 'FOREIGN KEY']);
      }).where('cu.TABLE_NAME', tableName).where('cu.TABLE_SCHEMA', schemaName).catch(console.error);
    var pkeys = keys.filter(p => p['CONSTRAINT_TYPE'] === 'PRIMARY KEY');
    var fkeys = keys.filter(p => p['CONSTRAINT_TYPE'] === 'FOREIGN KEY');

    var pkey = pkeys[0];
    if (pkey && pkey.COLUMN_NAME) {
      columns = columns.map(item => {
        if (item.name === pkey.COLUMN_NAME) {
          item.isprimary = true;
        }
        return item;
      })
    }
    var cindex = 0;
    for (var c of columns) {
      if (c && c.name) {
        var fkey = fkeys.filter(fk => fk['COLUMN_NAME'] === c.name);
        if (fkey.length > 0) {
          c.isforeign = true;
          c.foreignTable = fkey[0]['CONSTRAINT_NAME'][1].split('_').reverse()[0];
        }
      }
      columns[cindex] = c;
      cindex++;
    }
    return columns.sort((a, b) => Math.sign(a.position - b.position));
  }
  return [];
}
/**
 * 
 * @param {knex} knex 
 * @param {String} schemaName 
 * @returns {Table[]}
 */
async function getTables(knex, schemaName = 'dbo') {
  const client = knex.context.client.config.client.toLowerCase();
  if (client === 'mssql' || client === 'mysql') {
    var tables = await knex("INFORMATION_SCHEMA.TABLES").where({
      'TABLE_SCHEMA': schemaName
    }).then(results => results.map(item => new Table({
      name: item['TABLE_NAME'],
      type: item['TABLE_TYPE'],
      schema: item['TABLE_SCHEMA'],
      db: item['TABLE_CATALOG']
    }))).catch(console.error);
    return tables;
  }
  return [];
}
/**
 * 
 * @param {knex} knex 
 * @param {String} schemaName 
 * @returns {StoredProcedure[]}
 */
async function getStoredProcedures(knex, schemaName = 'dbo') {
  const client = knex.context.client.config.client.toLowerCase();
  if (client === 'mssql' || client === 'mysql') {
    var spParameters = await knex("INFORMATION_SCHEMA.PARAMETERS").where({
      'SPECIFIC_SCHEMA': schemaName
    }).catch(console.error);
    var storedProcedures = await knex("INFORMATION_SCHEMA.ROUTINES").where({
      'SPECIFIC_SCHEMA': schemaName
    }).then(results => results.map(item => new StoredProcedure({
      name: item['SPECIFIC_NAME'],
      schema: item['SPECIFIC_SCHEMA'],
      db: item['SPECFIC_CATALOG'],
      type: item['ROUTINE_TYPE'],
      returnType: item['DATA_TYPE'] ? {
        name: item['DATA_TYPE'],
        length: item['CHARACTER_MAXIMUM_LENGTH'],
        octetLength: item['CHARACTER_OCTET_LENGTH']
      } : null,
      body: item['ROUTINE_DEFINITION'],
      bodyType: item['ROUTINE_BODY'],
      accessMode: item['SQL_DATA_ACCESS'],
      parameters: spParameters.filter(arg => arg['SPECIFIC_NAME'] === item['SPECIFIC_NAME'] && arg['SPECIFIC_CATALOG'] == item['SPECIFIC_CATALOG'])
        .map(arg => new StoredProcedureParameter({
          name: arg['PARAMETER_NAME'],
          mode: arg['PARAMETER_MODE'],
          isResult: Boolean(arg['IS_RESULT'] === 'YES'),
          type: arg['DATA_TYPE'],
          length: arg['CHARACTER_MAXIMUM_LENGTH'],
          octetLength: arg['CHARACTER_OCTET_LENGTH']
        }))
    }))).catch(console.error);
    return storedProcedures;
  }
  return [];
}
module.exports = {
  getColumns,
  getTables,
  getStoredProcedures
};
