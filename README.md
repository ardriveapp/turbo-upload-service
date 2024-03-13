# Turbo Upload Service

Welcome to the Turbo Upload Service 👋

## Setting up the development environment

### System Package Installation

For a compatible development environment, we require the following packages installed on the system:

- `nvm`
- `yarn`
- `husky`
- `docker`

### Running the Upload Service locally

With a compatible system, follow these steps to start the upload service:

- `cp .env.sample .env` (and update values)
- `yarn`
- `yarn build`
- `yarn db:up && yarn db:migrate:latest`
- `yarn start`

Developers can alternatively use `yarn start:watch` to run the app in development mode with hot reloading provided by `nodemon`

## Database

### Scripts

- `yarn db:up`: Starts a local docker PostgreSQL container on port 5432
- `yarn db:down`: Tears down local docker PostgreSQL container and deletes the db volume
- `yarn db:migrate:list` - lists all the migrations applied to the database
- `yarn db:migrate:up MIGRATION_NAME`: Runs a specific migration on a local PostgreSQL database
- `yarn db:migrate:latest`: Runs migrations on a local PostgreSQL database
- `yarn db:migrate:new MIGRATION_NAME`: Generates a new migration file
- `yarn dotenv -e .env.dev ...`: run any of the above commands against a specific environment file

### Migrations

Knex is used to create and run migrations. To make a migration follow these steps:

1. Add migration function and logic to `schema.ts`
2. Run the yarn command to stage the migration, which generates a new migration script in `migrations/` directory

   - `yarn db:migrate:new MIGRATION_NAME` (e.g. `yarn db:migrate:new add_id_to_table`)

3. Construct the migration queries in [src/db/arch/migrator.ts](src/db/arch/migrator.ts)

4. Update the generated migration file to call the proper migration script.

5. Run the migration:

   - `yarn db:migration:latest` or `yarn knex migration:up MIGRATION_NAME.TS`

6. Alternatively, run the migration against a specific environment file:

   - `yarn dotenv -e .env.dev yarn db:migrate:latest`

### Rollbacks

You can rollback knex migrations using the following command:

- `yarn db:migrate:rollback` - rolls back the most recent migration
- `yarn db:migrate:rollback --all` - rolls back all migrations
- `yarn db:migrate:down MIGRATION_NAME` - rolls back a specific migration

Additional `knex` documentation can be found [here](https://knexjs.org/guide/migrations.html).

## Docker

### Building Image

To build the container:

```shell
docker build --build-arg NODE_VERSION=$(cat .nvmrc |cut -c2-8) --build-arg NODE_VERSION_SHORT=$(cat .nvmrc |cut -c2-3) .
```

### Docker Compose

Runs this service, against the most recent version of `payment-service` and `arlocal`, and local postgres instances.

```shell
docker compose up -d
```

Run just the upload service against migrated local postgres instance.

```shell
docker compose up upload-service --build
```

## Tests

Unit and integration tests can be run locally or via docker. For either, you can set environment variables for the service via a `.env` file:

- `yarn test:local` - runs unit and integration tests locally against postgres and arlocal docker containers

### Unit Tests

- `yarn test:unit` - runs unit tests locally

### Integration Tests

- `yarn test:integration:local` - runs the integration tests locally against postgres and arlocal docker containers
- `yarn test:integration:local -g "Router"` - runs targeted integration tests against postgres and arlocal docker containers
  - `watch -n 30 'yarn test:integration:local -g "Router'` - runs targeted integration tests on an interval (helpful when actively writing tests)
- `yarn test:docker` - runs integration tests (and unit tests) in an isolated docker container
