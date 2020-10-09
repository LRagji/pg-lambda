// Hand written type definitions for pg-que 0.0.4
import * as pg from 'pg-promise';
import { PgQueue } from 'pg-que';

interface StateStore {
    readerPG: pg.IDatabase<any>,
    writerPG: pg.IDatabase<any>
}

interface StartOptions {
    maxsteps: number,
    readFrequency: number,
    messageAcquiredTimeout: number,
    retry: number
}

export default class PgLambda {
    constructor(name: string, inputQ: PgQueue, outputQ: PgQueue, expression: string, stateStore: StateStore, workers?: number);
    startProcessing(options?: StartOptions): Promise<void>;
    stopProcessing(): Promise<void>;
    dispose(): Promise<void>;
    state(): Promise<Object>;
}