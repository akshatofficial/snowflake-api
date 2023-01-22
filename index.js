const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();
const snowflake = require('snowflake-sdk');

const connection = snowflake.createConnection({
  account: 'EGUIQEA-QL86552',
  username: 'chiragsuper123',
  password: 'Chiragsuper123@',
  application: 'Super-Sql',
  authenticator: 'SNOWFLAKE',
});

connection.connect(function (err, conn) {
  if (err) {
    console.log(err);
    console.error('Unable to connect: ' + err.message);
  } else {
    console.log('Successfully connected to Snowflake.');
    // Optional: store the connection ID.
    connection_ID = conn.getId();
  }
});

app.use(cors());
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

app.get('/databases', (req, res) => {
  let isError, errorMessage, schemas;
  connection.execute({
    sqlText: `show databases;`,
    complete: function (err, stmt, rows) {
      if (err) {
        res.status(400).send(err.message);
        isError = true;
        errorMessage = err.message;
        console.error(
          'Failed to execute statement due to the following error: ' +
            err.message,
        );
      } else {
        res.send(rows);
        isError = false;
        schemas = rows;
        console.log(rows);
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    },
  });
});

app.get('/database-schemas', (req, res) => {
  const { databaseName } = req.query;
  let isError, errorMessage, schemas;
  connection.execute({
    sqlText: `show schemas in ${databaseName}`,
    complete: function (err, stmt, rows) {
      if (err) {
        res.status(400).send(err.message);
        isError = true;
        errorMessage = err.message;
        console.error(
          'Failed to execute statement due to the following error: ' +
            err.message,
        );
      } else {
        res.send(rows);
        isError = false;
        schemas = rows;
        console.log(rows);
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    },
  });

  //   if (isError) {
  //     res.status(400).json({ message: errorMessage });
  //     return;
  //   }
  //   res.send(schemas);
});

app.get('/database-schema-tables', (req, res) => {
  // const schemaNames = req.body;
  const { schemaName, databaseName } = req.query;
  //   console.log(schemaName);
  let isError, errorMessage, schemas;
  connection.execute({
    //   sqlText: `show tables in ${schemaNames.join(', ')}`,
    sqlText: `show tables in ${databaseName}.${schemaName}`,
    complete: function (err, stmt, rows) {
      if (err) {
        res.status(400).send(err.message);
        isError = true;
        errorMessage = err.message;
        console.error(
          'Failed to execute statement due to the following error: ' +
            err.message,
        );
      } else {
        res.send(rows);
        isError = false;
        schemas = rows;
        console.log(rows);
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    },
  });

  //   if (isError) {
  //     res.status(400).json({ message: errorMessage });
  //     return;
  //   }
  //   res.send(schemas);
});

app.get('/database-schema-views', (req, res) => {
  // const schemaNames = req.body;
  const { schemaName, databaseName } = req.query;
  //   console.log(schemaName);
  let isError, errorMessage, schemas;
  connection.execute({
    //   sqlText: `show tables in ${schemaNames.join(', ')}`,
    sqlText: `show views in ${databaseName}.${schemaName}`,
    complete: function (err, stmt, rows) {
      if (err) {
        res.status(400).send(err.message);
        isError = true;
        errorMessage = err.message;
        console.error(
          'Failed to execute statement due to the following error: ' +
            err.message,
        );
      } else {
        res.send(rows);
        isError = false;
        schemas = rows;
        console.log(rows);
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    },
  });

  //   if (isError) {
  //     res.status(400).json({ message: errorMessage });
  //     return;
  //   }
  //   res.send(schemas);
});

app.post('/create-data-model', (req, res) => {
  const { dataModelId, dataModelCols } = req.body;
  connection.execute({
    //   sqlText: `show tables in ${schemaNames.join(', ')}`,
    sqlText: `insert into TestDb.TestSchema.dataModels select $1 PARSE_JSON($2) from values (${dataModelId}, ${dataModelCols})`,
    complete: function (err, stmt, rows) {
      if (err) {
        res.status(400).send(err.message);
        // isError = true;
        // errorMessage = err.message;
        console.error(
          'Failed to execute statement due to the following error: ' +
            err.message,
        );
      } else {
        res.send(rows);
        // isError = false;
        schemas = rows;
        // console.log(rows);
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    },
  });
});
app.listen(1001, () => console.log('App is running on port 1001'));
