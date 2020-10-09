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
const RunningAverageLambda = new lambdaType("Bit", inputQ, outputQ, runningAverageExpression, stateStore);

let counter = 1000;
main = async () => {
    console.time("Data Ingested");
    while (counter > 0) {
        await inputQ.enque([{ "data": counter }]);
        counter--;
    }
    console.timeEnd("Data Ingested");

    let lastState;
    let state
    let delayCounter = 1;
    do {
        lastState = state;
        await RunningAverageLambda.startProcessing();
        await sleep(5000);
        await RunningAverageLambda.stopProcessing();
        state = await RunningAverageLambda.state();
        console.log(`State after ${5 * delayCounter} Seconds: ${JSON.stringify(state)}`);
        delayCounter++;
    }
    while (JSON.stringify(state) != JSON.stringify(lastState));
}

validate = async () => {
    counter = 1000;
    let matches = counter;
    let expected;
    let summed = counter;
    let continueLoop;
    do {
        let payload = await outputQ.tryDeque();
        continueLoop = payload !== undefined;
        if (!continueLoop) continue;
        let actual = parseFloat(payload.Payload.data);
        continueLoop = !Number.isNaN(actual);
        if (!continueLoop) continue;
        expected = summed / ((1000 - counter) === 0 ? 1 : ((1000 - counter) + 1));
        if (expected != actual) {
            console.warn(`Expected: ${expected} Actual: ${actual}`);
        }
        else {
            matches--;
        }
        counter--;
        summed += counter;
        if (await outputQ.tryAcknowledge(payload.AckToken) == false) {
            console.log("Ack failed");
        }
    }
    while (continueLoop)
    console.log("Pending Matches:" + matches);
};

console.time("Application");
main()
    .finally(() => {
        console.timeEnd("Application");
        console.time("Validate");
        validate()
            .finally(() => {
                console.timeEnd("Validate");
                console.time("Dispose");
                RunningAverageLambda.dispose()
                    .finally(() => {
                        pgp.end();
                        console.timeEnd("Dispose");
                    });
            });
    });