-- The rows pgtasks 0.1.0 left in every state (UpgradeSeed), for SchemaUpgradeTest.
-- Written by SchemaSnapshot; never edit it.

INSERT INTO ${pgtasks_schema}.topic OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.topic, $seed$[
{"id":1,"name":"upgrade","retention":{"succeeded": "P30D"},"created_at":"2026-11-27T19:36:29.360267+01:00"}
]$seed$);

INSERT INTO ${pgtasks_schema}.node OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.node, $seed$[
{"id":1,"name":"old-node","version":"test","started_at":"2026-11-27T19:36:29.361395+01:00","heartbeat_at":"2026-11-27T19:36:29.361395+01:00","expiry":null}
]$seed$);

INSERT INTO ${pgtasks_schema}.task OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.task, $seed$[
{"id":1,"topic_id":1,"identifier":"succeeded","reference":null,"payload":"payload-succeeded","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.368529+01:00","outcome":"succeeded","previous_outcome":null,"output":null,"error":null,"attempts":1,"completed_at":"2026-11-27T19:36:29.374084+01:00"},
{"id":10,"topic_id":1,"identifier":"suspended","reference":null,"payload":null,"payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.391245+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null},
{"id":11,"topic_id":1,"identifier":"scheduled","reference":null,"payload":null,"payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.391743+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null},
{"id":2,"topic_id":1,"identifier":"filtered","reference":null,"payload":"payload-filtered","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.3754+01:00","outcome":"filtered","previous_outcome":null,"output":"not for us","error":null,"attempts":1,"completed_at":"2026-11-27T19:36:29.377754+01:00"},
{"id":3,"topic_id":1,"identifier":"dead","reference":null,"payload":"payload-dead","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.379712+01:00","outcome":null,"previous_outcome":null,"output":null,"error":{"message": "gave up"},"attempts":1,"completed_at":null},
{"id":4,"topic_id":1,"identifier":"retrying","reference":null,"payload":"payload-retrying","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.383852+01:00","outcome":null,"previous_outcome":null,"output":null,"error":{"message": "boom"},"attempts":1,"completed_at":null},
{"id":5,"topic_id":1,"identifier":"active","reference":null,"payload":"payload-active","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.386063+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null},
{"id":6,"topic_id":1,"identifier":"superseding","reference":null,"payload":"payload-superseding","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.387463+01:00","outcome":"redundant","previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":"2026-11-27T19:36:29.38913+01:00"},
{"id":7,"topic_id":1,"identifier":"superseding","reference":null,"payload":"payload-superseding","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.38913+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null},
{"id":8,"topic_id":1,"identifier":"ready","reference":null,"payload":"payload-ready","payload_type":null,"parent_id":null,"root_id":null,"headers":null,"created_at":"2026-11-27T19:36:29.389694+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null},
{"id":9,"topic_id":1,"identifier":"keyed","reference":"ref-7","payload":"{\"order\":7}","payload_type":"com.example.Order","parent_id":null,"root_id":null,"headers":{"tenant": "north", "traceparent": "00-abc-def-01"},"created_at":"2026-11-27T19:36:29.390723+01:00","outcome":null,"previous_outcome":null,"output":null,"error":null,"attempts":0,"completed_at":null}
]$seed$);

INSERT INTO ${pgtasks_schema}.queue OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.queue, $seed$[
{"task_id":10,"topic_id":1,"identifier":"suspended","state":"suspended","priority":0,"run_at":null,"attempt":0,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":0,"first_failed_at":null,"expiries":0},
{"task_id":11,"topic_id":1,"identifier":"scheduled","state":"scheduled","priority":0,"run_at":"2126-11-03T19:36:29.391444+01:00","attempt":0,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":0,"first_failed_at":null,"expiries":0},
{"task_id":3,"topic_id":1,"identifier":"dead","state":"dead_letter","priority":0,"run_at":null,"attempt":1,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":1,"first_failed_at":"2026-11-27T19:36:29.382725+01:00","expiries":0},
{"task_id":4,"topic_id":1,"identifier":"retrying","state":"retrying","priority":0,"run_at":"2026-11-27T19:36:29.385994+01:00","attempt":1,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":1,"first_failed_at":"2026-11-27T19:36:29.384994+01:00","expiries":0},
{"task_id":5,"topic_id":1,"identifier":"active","state":"active","priority":0,"run_at":null,"attempt":1,"node_id":1,"claimed_at":"2026-11-27T19:36:29.38681+01:00","dedup_key":null,"failures":0,"first_failed_at":null,"expiries":0},
{"task_id":7,"topic_id":1,"identifier":"superseding","state":"ready","priority":0,"run_at":null,"attempt":0,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":0,"first_failed_at":null,"expiries":0},
{"task_id":8,"topic_id":1,"identifier":"ready","state":"ready","priority":0,"run_at":null,"attempt":0,"node_id":null,"claimed_at":null,"dedup_key":null,"failures":0,"first_failed_at":null,"expiries":0},
{"task_id":9,"topic_id":1,"identifier":"keyed","state":"ready","priority":5,"run_at":null,"attempt":0,"node_id":null,"claimed_at":null,"dedup_key":"keyed-dedup","failures":0,"first_failed_at":null,"expiries":0}
]$seed$);

INSERT INTO ${pgtasks_schema}.topic_count OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.topic_count, $seed$[
{"topic_id":1,"taken_at":"2026-11-27T19:36:29.392282+01:00","scheduled":1,"retrying":1,"ready":3,"active":1,"suspended":1,"dead_letters":1,"due":1,"next_run_at":"2026-11-27T19:36:29.385994+01:00","oldest_ready_at":"2026-11-27T19:36:29.38913+01:00","failing":2,"oldest_failing_at":"2026-11-27T19:36:29.382725+01:00"}
]$seed$);

INSERT INTO ${pgtasks_schema}.lease OVERRIDING SYSTEM VALUE
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.lease, $seed$[
{"name":"housekeeping","node_id":1,"acquired_at":"2026-11-27T19:36:29.392282+01:00","expires_at":"2026-11-27T19:36:59.392282+01:00"}
]$seed$);

SELECT setval(pg_get_serial_sequence('${pgtasks_schema}.topic', 'id'), coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.topic;
SELECT setval(pg_get_serial_sequence('${pgtasks_schema}.node', 'id'), coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.node;
SELECT setval('${pgtasks_schema}.task_id_seq', coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.task;
