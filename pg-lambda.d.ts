// Hand written type definitions for pg-que 0.0.4
import * as pg from 'pg-promise';
import * as pgque from 'pg-que';

interface StateStore {
    readerPG: pg.IDatabase<any>,
    writerPG: pg.IDatabase<any>
}

interface StartOptions {
    maxsteps: integer,
    readFrequency: integer,
    messageAcquiredTimeout: integer,
    retry: integer
}

export default class PgLambda {
    constructor(name: string, inputQ: pgque, outputQ: pgque, expression: string, stateStore: StateStore, workers?: integer);
    async startProcessing(options?: StartOptions);
    async stopProcessing();
    async dispose();
}