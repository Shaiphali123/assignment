const { Client } = require('pg')
const dbOld = new Client({
  user: 'old',
  host: 'localhost',
  database: 'old',
  password: 'hehehe',
  port: 5432,
})


const dbNew = new Client({
  user: 'new',
  host: 'localhost',
  database: 'new',
  password: 'hahaha',
  port: 5433,
})


dbOld.connect(function (err) {
  if (err) throw err;
  console.log("Db old Connected!");

  dbNew.connect(function (err) {
    if (err) throw err;
    console.log("Db new Connected!");

    generateReport(dbOld, dbNew, 'accounts');
  });
});

async function fetchData(client, table) {
  const res = await client.query(`SELECT * FROM ${table}`);
  await client.end();
  return res.rows;
}

function compareDatasets(oldData, newData) {
  const report = {
    missingInNew: [],
    incorrectInNew: [],
    newRows: [],
  };

  const oldDataMap = new Map();
  const newDataMap = new Map();

  oldData.forEach(row => oldDataMap.set(row.id, row));
  newData.forEach(row => newDataMap.set(row.id, row));

  oldDataMap.forEach((oldRow, id) => {
    const newRow = newDataMap.get(id);
    if (!newRow) {
      report.missingInNew.push(oldRow);
    } else if (JSON.stringify(oldRow) !== JSON.stringify(newRow)) {
      report.incorrectInNew.push({ oldRow, newRow });
    }
  });

  newDataMap.forEach((newRow, id) => {
    if (!oldDataMap.has(id)) {
      report.newRows.push(newRow);
    }
  });

  return report;
}

async function generateReport(oldDb, newDb, table) {
  try {
    const oldData = await fetchData(oldDb, table);
    const newData = await fetchData(newDb, table);

    const report = compareDatasets(oldData, newData);

    console.log('Report:');
    console.log('Rows missing in new dataset:', report.missingInNew);
    console.log('Incorrect rows in new dataset:', report.incorrectInNew);
    console.log('New rows in new dataset:', report.newRows);
  } catch (error) {
    console.error('Error generating report:', error);
  }
}

