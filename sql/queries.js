const path = require('path');
const pgPromise = require('pg-promise');
const QueryFile = require('pg-promise').QueryFile;
const PreparedStatement = require('pg-promise').PreparedStatement;
const pgBootNS = require("pg-boot");
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
const schema0 = [
    {
        "file": sql('./v1/teraform.sql'),
        "params": []
    }
]
module.exports = (expressionName, expressionPK) => ({
    "FetchState": pgBootNS.PgBoot.dynamicPreparedStatement('FetchState', `SELECT jsonb_object_agg("Name",jsonb_build_object('value',"Value",'expiry',EXTRACT('epoch' from "Expiry")::Integer)) as "State"
        FROM $[expressionname:name]
        WHERE ("T-Stamped"+"Expiry") > (NOW() AT TIME ZONE 'UTC') OR "Expiry" IS NULL;`, { "expressionname": expressionName }),
    "SaveState": pgBootNS.PgBoot.dynamicPreparedStatement('SaveState', `INSERT INTO $[expressionname:name] ("Name","Value","Expiry")
    SELECT "key" as "Name", ("value" -> 'value')::JSONB as "Value", (("value" ->> 'expiry')::integer * INTERVAL '1 Second') as "Interval" FROM 
        jsonb_each($1::JSONB)
    ON CONFLICT ON CONSTRAINT $[expressionPK:name]
    DO UPDATE SET "Value" = EXCLUDED."Value","Expiry" = EXCLUDED."Expiry"`, { "expressionname": expressionName, "expressionPK": expressionPK }),
    "ClearVariables": pgBootNS.PgBoot.dynamicPreparedStatement('ClearVariables', `DELETE FROM $[expressionname:name] WHERE ("T-Stamped"+"Expiry") < (NOW() AT TIME ZONE 'UTC') AND "Expiry" IS NOT NULL ;`, { "expressionname": expressionName }),
    "Schema0": schema0,
})