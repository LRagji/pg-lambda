const mainThread = require("./worker");
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
module.exports = class WorkerManager {
    #workers = [];
    #pendingResponse = new Map();
    #iterations = 0;

    constructor(threadCount) {

        if (threadCount <= 0) {
            this.#workers.push({ "worker": { "terminate": () => { } }, "execute": mainThread });
        }
        else {
            while (threadCount > 0) {
                const worker = new Worker('./worker.js');
                this.#pendingResponse.set(worker.threadId, new Map());
                worker.on('message', (result) => {
                    this.#pendingResponse.get(worker.threadId).get(result.token).resolve(result);
                    this.#pendingResponse.get(worker.threadId).delete(result.token);
                });

                worker.on('error', (err) => {
                    if (worker.threadId === -1) return;
                    let responsesforCurrentThread = this.#pendingResponse.get(worker.threadId);
                    if (responsesforCurrentThread !== undefined) {
                        responsesforCurrentThread.forEach((value, key) => {
                            value.reject({ "token": key, "error": err });
                            this.#pendingResponse.get(worker.threadId).delete(key);
                        });
                    }
                    else {
                        console.log("No requests were found for " + worker.threadId + "  for error");
                    }
                });
                worker.on('exit', (code) => {
                    if (code !== 0) {
                        if (worker.threadId === -1) return;
                        let responsesforCurrentThread = this.#pendingResponse.get(worker.threadId);
                        if (responsesforCurrentThread !== undefined) {
                            responsesforCurrentThread.forEach((value, key) => {
                                value.reject({ "token": key, "error": new Error(`Worker stopped with exit code ${code}`) });
                                this.#pendingResponse.get(worker.threadId).delete(key);
                            });
                        }
                        else {
                            console.log("No requests were found for " + worker.threadId + "  for exit code " + code);
                        }
                    }
                });

                const execute = (params, token) => new Promise((resolve, reject) => {
                    try {
                        worker.postMessage([params, token]);
                        this.#pendingResponse.get(worker.threadId).set(token, { "resolve": resolve, "reject": reject });
                    }
                    catch (err) {
                        reject({ "token": token, "error": new Error(`Worker stopped with exit code ${code}`) });
                    }
                });
                this.#workers.push({ "worker": worker, "execute": execute });
                threadCount--;
            }
        }

        this.assign = this.assign.bind(this);
        this.dispose = this.dispose.bind(this);
    }

    async assign(params, token) {
        const workerId = ((this.#iterations % this.#workers.length));
        const worker = this.#workers[workerId];
        const returnPromise = worker.execute(params, (token || (`${workerId}-${this.#iterations}`)));
        if (this.#iterations > Number.MAX_SAFE_INTEGER) this.#iterations = 0;
        this.#iterations++;
        return returnPromise;
    }

    async dispose() {
        let allPromises = [];
        while (this.#workers.length > 0) {
            allPromises.push(this.#workers.pop().worker.terminate());
        }
        this.#pendingResponse.clear();
        return Promise.all(allPromises);
    }
}