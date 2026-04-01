# Testing

A Docker setup is provided for running the project locally.

## First-time setup

When creating the containers for the first time, run:

```bash
docker compose up --build
```

This command creates three containers:

- **Kafka**: starts the Kafka service
- **Producer**: runs `producer.py`
- **Consumer**: runs `consumer.py`

All three containers start automatically when you run the command above.

## Stopping the containers

To stop the containers:

```bash
docker compose down
```

## Rebuilding from scratch

If you want to stop the containers, remove their metadata, and build them again:

```bash
docker compose down -v
docker compose up --build
```

## Starting existing containers

If the containers have already been created, start them with:

```bash
docker compose up
```