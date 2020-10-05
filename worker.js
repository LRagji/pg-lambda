const { isMainThread, parentPort } = require('worker_threads');
const jsonata = require("jsonata");

if (isMainThread === false) {
    parentPort.on('message', (payload) => {
        const params = payload[0];
        const token = payload[1];
        queExecution(params, token).then((result) => parentPort.postMessage(result));
    });

};

function queExecution(params, token) {
    const jsonataScript = params[0];
    const input = params[1];
    const maxSteps = params[2];
    return new Promise((acc) => {
        let elapsed = Date.now();
        try {
            let stepCounter = 0;
            var expression = jsonata(jsonataScript);
            expression.evaluate(input, undefined,
                (err, result) => acc({ "token": token, "error": err, "result": result, "elapsed": (Date.now() - elapsed) }),
                async () => { stepCounter++; return stepCounter < maxSteps; });
        } catch (err) {
            acc({ "token": token, "error": err, "result": null, "elapsed": (Date.now() - elapsed) });
        }
    });
}

module.exports = queExecution;