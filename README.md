# pg-lambda

This package is an implementation of data transformational pipeline based on JSONata and PG for persistence
1. Que based Input and Output.
2. Timeouts for JSONata expressions.
3. Message persistence with postgres.
4. Can be used with H-Scaled services or with mutiple threads within a single process.
5. Pause and Resume execution.

## Getting Started

1. Install using `npm i pg-lambda`
2. Require in your project. `const QType = require('pg-lambda');`
3. Run postgres as local docker if required. `docker run --name pg-12.4 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=pg-queue -p 5432:5432 -d postgres:12.4-alpine`
4. Instantiate with a postgres readers and writers connection details. 
5. All done, Start using it!!.

## Examples/Code snippets

A complete example can be found at [here](https://raw.githubusercontent.com/LRagji/pg-lambda/master/examples/default.js)

1. **Initialize**
```javascript
const lambdaType = require('pg-lambda');
const QType = require('pg-que');
const pgp = require('pg-promise')();
pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.
pgp.pg.types.setTypeParser(1114, str => str); // UTC Timestamp Formatting Bug, 1114 is OID for timestamp in Postgres.
const defaultConectionString = "postgres://postgres:@localhost:5432/pg-queue";
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Lambda-Reader",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Lambda-Writer",
    max: 2 //2 Writer
};
const pgReader = pgp(readConfigParams);
const pgWriter = pgp(writeConfigParams);
const inputQ = new QType("iBit", pgReader, pgWriter);
const outputQ = new QType("oBit", pgReader, pgWriter);
const runningAverageExpression = `{
    "data":(($exists(pstep.state.sum.value)?pstep.state.sum.value:0)+data)/(($exists(pstep.state.count.value)?pstep.state.count.value:0)+1),
    "pstep":{
        "state":{
                "count":{
                    "value":($exists(pstep.state.count.value)?pstep.state.count.value:0)+1,
                    "expiry":null
                },
                "sum":{
                    "value":($exists(pstep.state.sum.value)?pstep.state.sum.value:0)+data,
                    "expiry":null
                }
            }
     }
}`;
const stateStore = { "readerPG": pgReader, "writerPG": pgWriter };
const BitFetcherLambda = new lambdaType("Bit", inputQ, outputQ, runningAverageExpression, stateStore);
```

2. **Start Processing**
```javascript
await BitFetcherLambda.startProcessing();
```
3. **Stop Processing**
```javascript
await BitFetcherLambda.stopProcessing()
```
4. **Dispose**
```javascript
await BitFetcherLambda.dispose();//Waits for current message processing to complete.
```

## Built with

1. Authors :heart: love :heart: for Open Source.
2. [pg-promise](https://www.npmjs.com/package/pg-promise).
3. [jsonata](https://www.npmjs.com/package/jsonata) Waiting on [PR](https://github.com/jsonata-js/jsonata/pull/478).
4. [pg-boot](https://www.npmjs.com/package/pg-boot)

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
0.0.1[Beta]

## License

This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.
