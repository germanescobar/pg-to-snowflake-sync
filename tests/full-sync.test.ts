import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Client } from 'pg';
import snowflake from 'snowflake-sdk';
import { runFullSync } from '../src/full-sync';
import { Config } from '../src/types';
import { connectToPostgresClient, connectToSnowflake, executeSnowflakeQuery } from '../src/utils';
import * as dotenv from 'dotenv';
import { loadConfig } from '../src/config';
import { recreateReplicationSlot } from './helpers';
import { recreatePublication } from './helpers';

dotenv.config({ path: '.env.test' });

describe('Full Sync Integration Test', () => {
  let pgClient: Client;
  let snowflakeConn: snowflake.Connection;
  let config: Config;

  beforeEach(async () => {
    config = loadConfig();

    // Connect to PostgreSQL
    pgClient = await connectToPostgresClient(config);

    // Recreate publication and replication slot
    await recreatePublication(pgClient, config.postgres.publicationName);
    await recreateReplicationSlot(pgClient, config.postgres.replicationSlot);

    // Create tables and insert mock data
    await pgClient.query(`
      DROP TABLE IF EXISTS posts, users;
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        settings JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      INSERT INTO users (name, email, settings) VALUES
      ('Alice', 'alice@example.com', '{"theme": "light"}'),
      ('Bob', 'bob@example.com', '{"theme": "dark"}');
    `);

    await pgClient.query(`
      CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        title VARCHAR(255),
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      INSERT INTO posts (user_id, title, content) VALUES
      (1, 'First Post', 'This is the first post content'),
      (2, 'Second Post', 'This is the second post content');
    `);

    // Connect to Snowflake
    snowflakeConn = await connectToSnowflake(config);
    await executeSnowflakeQuery(snowflakeConn, 'DROP TABLE IF EXISTS USERS');
    await executeSnowflakeQuery(snowflakeConn, 'DROP TABLE IF EXISTS POSTS');
  });

  afterEach(async () => {
    // Clean up PostgreSQL
    await pgClient.query('DROP TABLE IF EXISTS posts, users');
    await pgClient.end();

    // Clean up Snowflake
    await executeSnowflakeQuery(snowflakeConn, 'DROP TABLE IF EXISTS USERS');
    await executeSnowflakeQuery(snowflakeConn, 'DROP TABLE IF EXISTS POSTS');
    snowflakeConn.destroy(() => console.log('Snowflake connection closed'));
  });

  it('should sync data from PostgreSQL to Snowflake', async () => {
    // Run the full sync
    await runFullSync(config);

    // Verify data in Snowflake
    const usersResult = await executeSnowflakeQuery(snowflakeConn, 'SELECT * FROM USERS');
    const postsResult = await executeSnowflakeQuery(snowflakeConn, 'SELECT * FROM POSTS');

    expect(usersResult).toEqual([
      { ID: 1, NAME: 'Alice', EMAIL: 'alice@example.com', CREATED_AT: expect.any(Date), SETTINGS: {"theme": "light"} },
      { ID: 2, NAME: 'Bob', EMAIL: 'bob@example.com', CREATED_AT: expect.any(Date), SETTINGS: {"theme": "dark"} },
    ]);

    expect(postsResult).toEqual([
      { ID: 1, USER_ID: 1, TITLE: 'First Post', CONTENT: 'This is the first post content', CREATED_AT: expect.any(Date) },
      { ID: 2, USER_ID: 2, TITLE: 'Second Post', CONTENT: 'This is the second post content', CREATED_AT: expect.any(Date) },
    ]);
  });

  it('should exclude tables from sync', async () => {
    config.sync.excludedTables = ['posts'];
    // Run the full sync
    await runFullSync(config);

    // Verify data in Snowflake
    const usersResult = await executeSnowflakeQuery(snowflakeConn, 'SELECT * FROM USERS');

    expect(usersResult).toEqual([
      { ID: 1, NAME: 'Alice', EMAIL: 'alice@example.com', CREATED_AT: expect.any(Date), SETTINGS: {"theme": "light"} },
      { ID: 2, NAME: 'Bob', EMAIL: 'bob@example.com', CREATED_AT: expect.any(Date), SETTINGS: {"theme": "dark"} },
    ]);

    const postsTableExists = await executeSnowflakeQuery(snowflakeConn, `
      SELECT COUNT(*) AS table_count 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_NAME = 'POSTS'
    `);
    expect(postsTableExists[0].TABLE_COUNT).toBe(0);
  });
});