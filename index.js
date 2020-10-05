const pg = require('pg-promise');
const qType = require('pg-que');
const queries = require('./sql/queries');
const schemaVersion = "0.0.1";
const contractorType = require('./worker-manager');
const sleep = (sleepTime) => new Promise((a, r) => setTimeout(a, sleepTime));

module.exports = class PgLambda {

    #pgp;
    #readerPG;
    #writerPG;
    #schemaInitialized = false;
    #queries;
    #expressionName;
    #expressionNamePK;
    #expression;
    #inputQ;
    #outputQ;
    #timerHandle;
    #readFrequency;
    #contractor;
    #maxsteps;
    #currentOperation;

    constructor(name, inputQ, outputQ, expression, stateStore, workers = 0) {
        stateStore.schema = stateStore.schema || "public";
        const initOptions = {
            // query(e) {
            //     console.log(e.query);
            // },
            "schema": stateStore.schema
        };
        this.#pgp = pg(initOptions);
        this.#readerPG = this.#pgp(stateStore.readerPG);
        this.#writerPG = qType.checkForSimilarConnection(stateStore.readerPG, stateStore.writerPG, this.#readerPG, (c) => this.#pgp(c));
        this.#expressionName = "Lambda-" + name;
        this.#expressionNamePK = this.#expressionName + "-PK";
        this.#queries = queries(this.#expressionName, this.#expressionNamePK, stateStore.schema);
        this.#expression = expression;
        this.#inputQ = inputQ;
        this.#outputQ = outputQ;

        this.#contractor = new contractorType(workers);
        this.#initialize = this.#initialize.bind(this);
        this.#process = this.#process.bind(this);
        this.dispose = this.dispose.bind(this);
        this.startProcessing = this.startProcessing.bind(this);
        this.stopProcessing = this.stopProcessing.bind(this);
    }

    #initialize = async (version) => {
        if (this.#schemaInitialized === true) return;
        let someVersionExists = await this.#readerPG.one(this.#queries.VersionFunctionExists);
        if (someVersionExists.exists === true) {
            const existingVersion = await this.#readerPG.one(this.#queries.CheckSchemaVersion);
            if (existingVersion.LambdaVersion === version) {
                this.#schemaInitialized = true;
                return;
            }
        }

        await this.#writerPG.tx(async transaction => {
            let acquired = await transaction.one(this.#queries.TransactionLock, [("Lambda" + version)]);
            if (acquired.Locked === true) {
                for (let idx = 0; idx < this.#queries["Schema0.0.1"].length; idx++) {
                    let step = this.#queries["Schema0.0.1"][idx];
                    step.params.push(this.#expressionName);
                    step.params.push(this.#expressionNamePK);
                    step.params.push(version);
                    await transaction.none(step.file, step.params);
                };
            };
            return;
        });
        this.#schemaInitialized = true;
    }

    async startProcessing(maxsteps = 100, readFrequency = 1000) {
        maxsteps = parseInt(maxsteps);
        if (Number.isNaN(maxsteps)) throw new Error("Invalid maximum steps parameter " + maxsteps);
        readFrequency = parseInt(readFrequency);
        if (Number.isNaN(readFrequency)) throw new Error("Invalid readFrequency parameter " + readFrequency);

        await this.#initialize(schemaVersion);

        this.#readFrequency = readFrequency;
        this.#maxsteps = maxsteps;

        await this.#process();
    }

    async stopProcessing() {
        await this.#initialize(schemaVersion);
        if (this.#timerHandle != undefined) clearTimeout(timerHandle);
        return this.#currentOperation;
    }

    async dispose() {
        await this.#currentOperation;
        await this.#contractor.dispose();
        if (this.#inputQ) this.#inputQ = undefined;
        if (this.#outputQ) this.#outputQ = undefined;
        this.#pgp.end();
    }

    #process = () => {
        const fetch = async () => {
            const input = await this.#inputQ.tryDeque(10);
            if (input != undefined) {
                const state = await this.#readerPG.any(this.#queries.FetchState);
                if (input.Payload != undefined) {
                    if (input.Payload.result != undefined) {
                        input.Payload.result.state = state[0].State;
                    }
                    else {
                        input.Payload.result = { "state": state[0].State };
                    }
                }
                let output = await this.#contractor.assign([this.#expression, input, this.#maxsteps], input.AckToken);

                if (output != undefined && output.result != undefined) {
                    await this.#writerPG.any(this.#queries.SaveState, [output.result.state]);
                    await this.#writerPG.any(this.#queries.ClearVariables);
                    delete output.result.state;
                }
                await this.#outputQ.enque([output]);
                let ack = false;
                let sleepTime = 100;
                let retryCounter = 3;
                let ackElapsed = Date.now();
                while (ack === false) {
                    ack = await this.#inputQ.tryAcknowledge(input.AckToken);
                    if (ack === false && retryCounter > 0) {
                        await sleep(sleepTime);
                        sleepTime += sleepTime;
                        retryCounter--;
                        if (retryCounter <= 0) {
                            await this.#outputQ.enque([{ "elapsed": (Date.now() - ackElapsed), "error": `Failed to acknowledge ID: ${input.Id}, but processed the same.`, "result": null, "token": input.AckToken }]);
                            console.log(`Failed to acknowledge ID: ${input.Id}, but processed the same.`);
                            ack = true;
                        }
                    }
                }
            }
        };
        this.#currentOperation = fetch().then((v) => {
            this.#timerHandle = setTimeout(this.#process, this.#readFrequency);
        }).catch(console.error);
    }
}

//TODO:
//CHANGE QUERY FUNCTION NAME according to name of the lambda

// var babel = require("babel-core");

// let stepCounter = 0;
// let totalMemory = 0;
// const input = `($x:= λ($c,$n,$b){ $c=$b?$n%2:$x($c+1,$floor($n/2),$b)};$x(0,number,bitIndex))`;
// const expression = jsonata(input);

// let previousStepMemory = process.memoryUsage().heapUsed;
// let elapsed = Date.now();
// expression.evaluate({ "number": 10000000, "bitIndex": -1 }, undefined, completion, continueExecution);

// async function continueExecution() {
//     stepCounter++;
//     // let stepMemory = process.memoryUsage().heapUsed;
//     // totalMemory += stepMemory - previousStepMemory;
//     // previousStepMemory = stepMemory;
//     return stepCounter < 1000;
// }

// function completion(err, results) {
//     if (err) {
//         console.error(err);
//         return;
//     }
//     console.log(`Time:${Date.now() - elapsed} Steps:${stepCounter} Memory:${totalMemory} Result:${results}`);
// }

// //console.log(ast);


// const getBit = `($x:= λ($c,$n,$b){ $c=$b?$n%2:$x($c+1,$floor($n/2),$b)};$x(0,number,bitIndex))`;
// const input = { "number": 15, "bitIndex": -1 };
// const contractorType = require('./worker-manager');
// const contractor = new contractorType(5);

// let iterations = 100;
// let activeResquests = [];

// let elapsed = Date.now();
// while (iterations > 0) {
//     activeResquests.push(contractor.assign([getBit, input, 10000], `I-${iterations}`).then(console.log).catch(console.error));
//     iterations--;
// }

// Promise.all(activeResquests).then((r) => {
//     contractor.dispose();
//     console.log("Total Time:" + (Date.now() - elapsed));
// }).catch(console.error)


