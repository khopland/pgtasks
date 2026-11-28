-- pgtasks schema, version 3: the values extensions keep next to the tasks (ExtensionContext.data()),
-- so an extension needs no table, migration or store-specific code of its own. Released files are
-- never edited (docs/operations.md, "Changing the schema"); SchemaFreezeTest pins this one once
-- it ships.
--
-- Nodes of earlier versions still run against it, so compatible_from stays 1: they don't know the
-- table, and nothing of theirs reads or writes it. Creating a table takes no lock on the others.

-- One row per extension and key. The key's "C" collation orders keys by code point and lets a
-- prefix LIKE use the primary key's index. version starts at 1 and grows by one on each write,
-- for writes that only succeed if nobody wrote in between.
CREATE TABLE ${pgtasks_schema}.extension_data (
    extension   text COLLATE "C" NOT NULL CHECK (length(extension) BETWEEN 1 AND 200),
    key         text COLLATE "C" NOT NULL CHECK (length(key) BETWEEN 1 AND 200),
    value       text NOT NULL,
    version     bigint NOT NULL CHECK (version > 0),
    updated_at  timestamptz NOT NULL,
    PRIMARY KEY (extension, key)
);

INSERT INTO ${pgtasks_schema}.meta (singleton, version, compatible_from) VALUES (true, 3, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
