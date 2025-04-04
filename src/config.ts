import { Config } from "./types";

export function loadConfig() {
  return {
    postgres: {
      host: process.env.PG_HOST || '',
      port: parseInt(process.env.PG_PORT || '5432'),
      database: process.env.PG_DATABASE || '',
      user: process.env.PG_USER || '',
      password: process.env.PG_PASSWORD || '',
      replicationSlot: process.env.PG_REPLICATION_SLOT || 'snowflake_slot',
      publicationName: process.env.PG_PUBLICATION_NAME || 'snowflake_pub',
    },
    snowflake: {
      account: process.env.SF_ACCOUNT || '',
      username: process.env.SF_USERNAME || '',
      password: process.env.SF_PASSWORD || '',
      database: process.env.SF_DATABASE || '',
      schema: process.env.SF_SCHEMA || 'PUBLIC',
      warehouse: process.env.SF_WAREHOUSE || '',
      role: process.env.SF_ROLE || '',
    },
    sync: {
      excludedTables: (process.env.EXCLUDED_TABLES || '').split(',').filter(Boolean),
    }
  } as Config;
}