# kafka-tutorial — Topics, particiones y offsets

Práctica 3.2.1 de **Desarrollo Cloud Native I**: un productor REST y un consumidor `@KafkaListener` en Spring Boot
que muestran en qué **partición** y **offset** cae cada mensaje, con y sin key.

📘 **Guía completa:** [GUIA.md](GUIA.md)

## Inicio rápido

```bash
# 1. Kafka + ZooKeeper + Kafka UI
docker compose up -d

# 2. Crear el topic con 3 particiones
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
  --topic tutorial-particiones --partitions 3 --replication-factor 1

# 3. Levantar la app (puerto 8888)
mvn spring-boot:run
```

```bash
# Sin key
curl "http://localhost:8888/api/kafka/send?message=Mensaje1"
# Con key: siempre a la misma partición
curl "http://localhost:8888/api/kafka/send?message=Paso1-Creada&key=orden-555"
```

Kafka UI: <http://localhost:8080> · Pruebas sin Docker: `mvn test`

## Apagar

```bash
docker compose down
```
