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

- `db:up`: Starts a local docker PostgreSQL container on port 5432
- `db:migrate:latest`: Runs migrations on a local PostgreSQL database
- `db:down`: Tears down local docker PostgreSQL container and deletes the db volume

### Migrations

Knex is used to create and run migrations. To make a migration follow these steps:

1. Add migration function and logic to `schema.ts`
2. Run the yarn command to stage the migration, which generates a new migration script in `migrations/` directory

- `yarn db:make:migration MIGRATION_NAME`

3. Update the new migration to call the static function created in step 1.

4. Run the migration

- `yarn db:migration:latest` or `yarn knex migration:up MIGRATION_NAME.TS`

### Rollbacks

You can rollback knex migrations using the following command:

- `yarn db:migrate:rollback` - rolls back the most recent migration
- `yarn db:migrate:rollback --all` - rolls back all migrations
- `yarn knex migrate:list` - lists all the migrations applied to the database
- `yarn knex migrate:down MIGRATION_NAME.ts --knexfile src/database/knexfile.ts` - rolls back a specific migration

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

### Unit Tests

- `yarn test:unit` - runs unit tests locally

### Integration Tests

- `yarn test:integration:local` - runs the integration tests locally against postgres and arlocal docker containers
- `yarn test:integration:local -g "Router"` - runs targeted integration tests against postgres and arlocal docker containers
  - `watch -n 30 'yarn test:integration:local -g "Router'` - runs targeted integration tests on an interval (helpful when actively writing tests)
- `yarn test:docker` - runs integration tests (and unit tests) in an isolated docker container
