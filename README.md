# Postgres to Snowflake Sync

Sync data from Postgres to Snowflake using logical replication. It supports full and incremental synchronization.

You can run it as a Github Action, as a script (for full syncs and partial syncs) or as a service (for real-time, continuous sync).

## Usage

### Github Action

```yaml
name: Postgres to Snowflake Sync

on:
  workflow_dispatch:
    inputs:
      full-sync:
        description: 'Run a full sync'
        required: false
        default: false
        type: boolean
  schedule:
    - cron: '0 * * * *'  # Every hour, for every two hours use '0 */2 * * *'

jobs:
  sync:
    runs-on: ubuntu-latest
    # Prevent concurrent runs of the same workflow
    concurrency:
      group: sync-global
      cancel-in-progress: true
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3 
        with:
          node-version: 20
      - name: Run PostgreSQL to Snowflake Sync
        uses: germanescobar/pg-to-snowflake-sync@0.1.0
        with:
          args: ${{ inputs.full-sync && '--full-sync' || '' }}
        env:
          PG_HOST: ${{ secrets.PG_HOST }}
          PG_PORT: ${{ secrets.PG_PORT }}
          PG_DATABASE: ${{ secrets.PG_DATABASE }}
          PG_USER: ${{ secrets.PG_USER }}
          PG_PASSWORD: ${{ secrets.PG_PASSWORD }}
          SF_ACCOUNT: ${{ secrets.SF_ACCOUNT }}
          SF_USERNAME: ${{ secrets.SF_USERNAME }}
          SF_PASSWORD: ${{ secrets.SF_PASSWORD }}
          SF_DATABASE: ${{ secrets.SF_DATABASE }}
          SF_SCHEMA: ${{ secrets.SF_SCHEMA }}
          SF_WAREHOUSE: ${{ secrets.SF_WAREHOUSE }}
          SF_ROLE: ${{ secrets.SF_ROLE }}
          EXCLUDED_TABLES: 'table1, table2'
```

### Standalone Script

For full sync:

```bash
npm install
npm run start -- --full-sync
```

For incremental sync:

```bash
npm install
npm run start
```

### Service

For continuous incremental sync. 

Start the service:

```bash
npm install
npm run start:service
```

To stop the service:

```bash
npm run stop:service
```

Check the status of the service:

```bash
npm run service:status
```

Monitor the service:

```bash
npm run service:monitor
```

Check the logs of the service:

```bash
npm run service:logs
```

## Snowflake Authentication

The recommended way to authenticate with Snowflake is using a key pair (as they are requiring MFA for username/password authentication).

You can generate a key pair using the following commands:

```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key from private key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

You will need to register the public key in Snowflake:

```sql
-- Run this in Snowflake
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<paste_public_key_here>';
```

Replace `<your_username>` with your Snowflake username and `<paste_public_key_here>` with the content of your `rsa_key.pub` file (including the `BEGIN` and `END` lines).

Use the `SF_PRIVATE_KEY_PATH` environment variable to provide the path to the private key file.

You can also use the `SF_PRIVATE_KEY_PASSPHRASE` environment variable to provide a passphrase for the private key file.
