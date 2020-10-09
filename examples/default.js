const lambdaType = require('../pg-lambda');
const initOptions = {
    // query(e) {
    //     console.log(e.query);
    // },
    "schema": "Lambda"
};
const pgp = require('pg-promise')(initOptions);
pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.
pgp.pg.types.setTypeParser(1114, str => str); // UTC Timestamp Formatting Bug, 1114 is OID for timestamp in Postgres.
const QType = require('pg-que');
const defaultConectionString = "postgres://postgres:@localhost:5432/QUEUE";
const sleep = (sleepTime) => new Promise((a, r) => setTimeout(a, sleepTime));

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
const bitExpression = `($x:= λ($c,$n,$b){ $c=$b?$n%2:$x($c+1,$floor($n/2),$b)};$x(0,number,bitIndex))`;
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

main = async () => {
    let ctr = 1000;
    console.log("Expected answer: " + ((ctr * (ctr + 1) / 2) / ctr));
    while (ctr > 0) {
        await inputQ.enque([{ "data": ctr }]);
        ctr--;
    }
    await BitFetcherLambda.startProcessing();
    //await sleep(2000);
    //(ctr*(ctr+1))/ctr
}

main();
// console.time("Application");
// main().then((r) => {
//     BitFetcherLambda.stopProcessing().finally(() => {
//         console.timeEnd("Application");
//         BitFetcherLambda.dispose();
//         inputQ.dispose();
//         outputQ.dispose();
//     })
// })

// {
//     "output":(($exists(state.sum.value)?state.sum.value:0)+input)/(($exists(state.count.value)?state.coun.valuet:0)+1),
//     "state":{
//         "count":{
//             "value":($exists(state.count.value)?state.coun.valuet:0)+1,
//             "expiry":100
//         },
//         "sum":{
//             "value":($exists(state.sum.value)?state.sum.value:0)+input,
//             "expiry":100
//         }
//     }
// }

// OUTPUT
// {
//     "output": 2,
//     "state": {
//         "count": {
//             "value": 1,
//             "expiry": 100
//         },
//         "sum": {
//             "value": 2,
//             "expiry": 100
//         }
//     }
// }

//FORMAT
// {
//     "input": 1,
//     "state": {
//       "count": {
//         "value": 1,
//         "expiry": 100
//       },
//       "sum": {
//         "value": 1,
//         "expiry": 100
//       }
//     }
//   }