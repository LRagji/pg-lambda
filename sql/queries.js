const path = require('path');
const pgPromise = require('pg-promise');
const QueryFile = require('pg-promise').QueryFile;
const PreparedStatement = require('pg-promise').PreparedStatement;
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
module.exports = (expressionName, expressionPK, schema, lvf) => ({
    "TransactionLock": new PreparedStatement({ name: 'TransactionLock', text: `SELECT pg_advisory_xact_lock(hashtext($1)) as "Locked";` }),
    "FetchState": new PreparedStatement({
        name: 'FetchState', text: pgPromise.as.format(`SELECT jsonb_object_agg("Name",jsonb_build_object('value',"Value",'expiry',EXTRACT('epoch' from "Expiry")::Integer)) as "State"
        FROM $[expressionname:name]
        WHERE ("T-Stamped"+"Expiry") > (NOW() AT TIME ZONE 'UTC') OR "Expiry" IS NULL;`, { "expressionname": expressionName })
    }),
    "SaveState": new PreparedStatement({
        name: 'SaveState', text: pgPromise.as.format(`INSERT INTO $[expressionname:name] ("Name","Value","Expiry")
    SELECT "key" as "Name", ("value" -> 'value')::JSONB as "Value", (("value" ->> 'expiry')::integer * INTERVAL '1 Second') as "Interval" FROM 
        jsonb_each($1::JSONB)
    ON CONFLICT ON CONSTRAINT $[expressionPK:name]
    DO UPDATE SET "Value" = EXCLUDED."Value","Expiry" = EXCLUDED."Expiry"`, { "expressionname": expressionName, "expressionPK": expressionPK })
    }),
    "ClearVariables": new PreparedStatement({
        name: 'ClearVariables', text: pgPromise.as.format(`DELETE FROM $[expressionname:name] WHERE ("T-Stamped"+"Expiry") < (NOW() AT TIME ZONE 'UTC') AND "Expiry" IS NOT NULL ;`, { "expressionname": expressionName })
    }),
    "VersionFunctionExists": new PreparedStatement({
        name: 'VersionFunctionExistsL', text: pgPromise.as.format(`SELECT EXISTS(
        SELECT 1 
        FROM pg_catalog.pg_proc p
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace 
        WHERE proname = $[LVF] AND n.nspname::TEXT=$[schema])`, { "schema": schema, "LVF": lvf })
    }),
    "CheckSchemaVersion": new PreparedStatement({ name: 'CheckSchemaVersionL', text: pgPromise.as.format(`SELECT $[LVF:name]() AS "LambdaVersion";`, { "LVF": lvf }) }),
    "Schema0.0.1": [
        {
            "file": sql('./v1/teraform.sql'),
            "params": []
        }
    ],
})