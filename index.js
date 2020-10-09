const pg = require('pg-promise');
const qType = require('pg-que');
const crypto = require('crypto');
const queries = require('./sql/queries');
const schemaVersion = "0.0.1";
const contractorType = require('./worker-manager');
const sleep = (sleepTime) => new Promise((a, r) => setTimeout(a, sleepTime));
const previousStep = 'pstep';

module.exports = class PgLambda {

    #readerPG;
    #writerPG;
    #schemaInitialized = false;
    #queries;
    #expressionName;
    #expressionNamePK;
    #LambdaVersionFunctionName;
    #expression;
    #inputQ;
    #outputQ;
    #timerHandle;
    #contractor;
    #processOptions
    #currentOperation;

    constructor(name, inputQ, outputQ, expression, stateStore, workers = 0) {

        name = crypto.createHash('md5').update(name).digest('hex');

        this.#readerPG = stateStore.readerPG;
        this.#writerPG = stateStore.writerPG;
        this.#expressionName = "L-" + name;
        this.#expressionNamePK = this.#expressionName + "-PK";
        this.#LambdaVersionFunctionName = "LVF-" + name;
        this.#queries = queries(this.#expressionName, this.#expressionNamePK, (this.#readerPG.$config.options.schema || 'public'), this.#LambdaVersionFunctionName);
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


        await this.#writerPG.tx(async transaction => {
            await transaction.one(this.#queries.TransactionLock, [("Lambda" + version)]);

            let someVersionExists = await this.#readerPG.one(this.#queries.VersionFunctionExists);
            if (someVersionExists.exists === true) {
                const existingVersion = await this.#readerPG.one(this.#queries.CheckSchemaVersion);
                if (existingVersion.LambdaVersion === version) {
                    this.#schemaInitialized = true;
                    return;
                }
            }

            for (let idx = 0; idx < this.#queries.Schema0.length; idx++) {
                let step = this.#queries.Schema0[idx];
                step.params.push(this.#expressionName);
                step.params.push(this.#expressionNamePK);
                step.params.push(version);
                step.params.push(this.#LambdaVersionFunctionName);
                await transaction.none(step.file, step.params);
            };
        });
        this.#schemaInitialized = true;
    }

    async startProcessing(options = { maxsteps: 100, readFrequency: 1000, messageAcquiredTimeout: 3600, retry: 10 }) {

        options.maxsteps = parseInt(options.maxsteps);
        if (Number.isNaN(options.maxsteps) || options.maxsteps < 1) throw new Error("Invalid maximum steps parameter " + options.maxsteps);

        options.readFrequency = parseInt(options.readFrequency);
        if (Number.isNaN(options.readFrequency) || options.readFrequency < 100) throw new Error("Invalid readFrequency parameter " + options.readFrequency);

        options.messageAcquiredTimeout = parseInt(options.messageAcquiredTimeout);
        if (Number.isNaN(options.messageAcquiredTimeout) || options.messageAcquiredTimeout < 10) throw new Error("Invalid messageAcquiredTimeout parameter " + options.messageAcquiredTimeout);

        options.retry = parseInt(options.retry);
        if (Number.isNaN(options.retry) || options.retry < 1) throw new Error("Invalid retry parameter " + options.retry);

        await this.#initialize(schemaVersion);

        this.#processOptions = options;
        this.#processOptions.stopExecution = false;

        this.#timerHandle = setTimeout(this.#process, 100);
    }

    async stopProcessing() {
        this.#processOptions.stopExecution = true;
        await this.#initialize(schemaVersion);
        if (this.#timerHandle != undefined) clearTimeout(this.#timerHandle);
        return this.#currentOperation;
    }

    async dispose() {
        await this.#currentOperation;
        await this.#contractor.dispose();
        if (this.#inputQ) this.#inputQ = undefined;
        if (this.#outputQ) this.#outputQ = undefined;
    }

    #process = () => {
        const fetch = async () => {
            try {
                let input;
                do {
                    input = await this.#inputQ.tryDeque(this.#processOptions.messageAcquiredTimeout, this.#processOptions.retry);
                    if (input != undefined) {
                        const state = await this.#readerPG.any(this.#queries.FetchState);

                        if (input.Payload != undefined && input.Payload[previousStep] != undefined) {
                            input.Payload[previousStep].state = state[0].State;
                        }
                        else {
                            input.Payload[previousStep] = { "state": state[0].State };
                        }

                        let output = await this.#contractor.assign([this.#expression, input.Payload, this.#processOptions.maxsteps], input.AckToken);

                        if (output != undefined && output.result != undefined && output.result[previousStep] != undefined) {
                            await this.#writerPG.any(this.#queries.SaveState, [output.result[previousStep].state]);
                            await this.#writerPG.any(this.#queries.ClearVariables);
                            if (output.result[previousStep].state != undefined) delete output.result[previousStep].state;
                        }
                        else if (output.result == undefined) {
                            output.result = { [previousStep]: {} };
                        }
                        else if (output.result[previousStep] == undefined) {
                            output.result[previousStep] = {};
                        }
                        else {
                            output = { "result": { [previousStep]: {} } };
                        }
                        output.result[previousStep].error = output.error;
                        output.result[previousStep].elapsed = output.elapsed;
                        await this.#outputQ.enque([output.result]);
                        let ack = false;
                        let sleepTime = 100;
                        let retryCounter = this.#processOptions.retry;

                        while (ack === false) {
                            ack = await this.#inputQ.tryAcknowledge(input.AckToken, this.#processOptions.retry);
                            if (ack === false && retryCounter > 0) {
                                await sleep(sleepTime);
                                sleepTime += sleepTime;
                                retryCounter--;
                                if (retryCounter <= 0) {
                                    console.error(`Failed to acknowledge ID: ${input.Id}, but processed the same.`); //We may need and error QUE
                                    ack = true;
                                }
                            }
                        }
                    }
                }
                while (this.#processOptions.stopExecution === false && input != undefined);
            }
            catch (err) {
                console.error(`Failed while processing: ${err}`); //We may need and error QUE
            }
        };
        this.#currentOperation = fetch().finally((result) => {
            if (this.#processOptions.stopExecution === false) {
                this.#timerHandle = setTimeout(this.#process, this.#processOptions.readFrequency);
            }
        });
    }
}