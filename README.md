# streaming-dotnet

An example repo of how to use Chr.Avro with Confluent Kafka libraries that allows the Producer and Consumer to be built around a base class or interface for the events.

Serialisers are built for each event type and managed by the `AsyncSchemaRegistryGenericEventSerializer`

## Nodatime
Custom Schema, Serializer and Deserializer cases show how to use LocalDate and Instant for Avro logical types of date and timestamp while preserving the existing behaviour.
