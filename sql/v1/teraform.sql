CREATE TABLE $1:name
(
    "Id" bigserial NOT NULL,
    "T-Stamped" timestamp without time zone NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    "Name" text NOT NULL,
    "Expiry" interval,
    "Value" jsonb,
    CONSTRAINT $2:name PRIMARY KEY ("Name")
)WITH (FILLFACTOR = 50);
