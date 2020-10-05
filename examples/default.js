const lambdaType = require('../index');
const QType = require('/Users/laukikragji/Documents/Git/Personal/pg-queue/index.js');
const defaultConectionString = "postgres://postgres:@localhost:5432/QUEUE";
const sleep = (sleepTime) => new Promise((a, r) => setTimeout(a, sleepTime));
const schema = "Test";
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Example1-Queue-Reader",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Example1-Queue-Writer",
    max: 2 //2 Writer
};

const inputQ = new QType("iBit", readConfigParams, writeConfigParams, schema);
const outputQ = new QType("oBit", readConfigParams, writeConfigParams, schema);
const bitExpression = `($x:= λ($c,$n,$b){ $c=$b?$n%2:$x($c+1,$floor($n/2),$b)};$x(0,number,bitIndex))`;
const runningAverage = `{
    "input":(($exists(result.state.sum.value)?result.state.sum.value:0)+result.input)/(($exists(result.state.count.value)?result.state.coun.value:0)+1),
    "state":{
        "count":{
            "value":($exists(result.state.count.value)?result.state.coun.value:0)+1,
            "expiry":100
        },
        "sum":{
            "value":($exists(result.state.sum.value)?result.state.sum.value:0)+result.input,
            "expiry":100
        }
    }
}`;
const stateStore = { "readerPG": readConfigParams, "writerPG": writeConfigParams, "schema": schema };
const BitFetcherLambda = new lambdaType("Bit", inputQ, outputQ, runningAverage, stateStore);

main = async () => {
    await inputQ.enque([{ "result": { "input": 1 } }]);
    await BitFetcherLambda.startProcessing();
    //await sleep(200000);
}

main();
// console.time("Application");
// main().then((r) => {
//     BitFetcherLambda.stopProcessing();
//     console.timeEnd("Application");
//     BitFetcherLambda.dispose();
//     inputQ.dispose();
//     outputQ.dispose();
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