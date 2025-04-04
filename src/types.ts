export type Config = {
  postgres: {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
    replicationSlot: string;
    publicationName: string;
  }
  snowflake: {
    account: string;
    username: string;
    password: string;
    database: string;
    schema: string;
    warehouse: string;
    role: string;
  }
  sync: {
    excludedTables: string[];
  }
}

// Schema cache and helper function types
export type SnowflakeColumn = {
  name: string;
  dataType: string;
  isNullable: boolean;
};

export type SnowflakeTable = {
  columns: Map<string, SnowflakeColumn>;
};

export type SchemaCache = Map<string, SnowflakeTable>;

export type SchemaUpdateFunction = (relation: any) => Promise<void>;