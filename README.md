Prueba de concepto Kafka-Storm usando serialización de datos con Avro.

El cliente Java se conecta a un Schema-Repo para obtener el esquema y construir el mensaje serializado. Posteriormente se publica el mensaje en la cola avrotopic de kafka.

Se ha creado una topología con un Spout responsable de leer los mensajes publicados en la cola y que han sido serializados con Avro en origen. Posteriormente se pasa por un Bolt que de nuevo obtiene el esquema del repositorio Schema-Repo para reconstruir el objeto JSON.
