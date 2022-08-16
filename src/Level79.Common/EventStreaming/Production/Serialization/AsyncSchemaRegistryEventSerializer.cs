using Chr.Avro.Abstract;
using Chr.Avro.Confluent;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Level79.Common.EventStreaming.Production.SchemaBuilderCases;

namespace Level79.Common.EventStreaming.Production.Serialization;

public class AsyncSchemaRegistryEventSerializer<T> : IAsyncSerializer<T> where T : IEvent
{
    private readonly string _subject;
    private readonly SchemaRegistrySerializerBuilder _serializerBuilder;
    private ISerializer<T>? _serializer;

    public AsyncSchemaRegistryEventSerializer(ISchemaRegistryClient schemaRegistry)
    {
        _subject = typeof(T).FullName?.ToLowerInvariant() ?? "UnknownSubject";

        const TemporalBehavior temporalBehavior = TemporalBehavior.EpochMicroseconds;
        
        var schemaBuilder = new SchemaBuilder(
            SchemaBuilder.CreateDefaultCaseBuilders(temporalBehavior: temporalBehavior)
            .Prepend(_ => new InstantSchemaBuilderCase(temporalBehavior))
            .Prepend(_ => new LocalDateSchemaBuilderCase(temporalBehavior))
        );

        _serializerBuilder = new SchemaRegistrySerializerBuilder(
            schemaRegistry,
            schemaBuilder,
            serializerBuilder: new BinarySerializerBuilder(
                BinarySerializerBuilder.CreateDefaultCaseBuilders()
                .Prepend(_ => new BinaryInstantSerializerBuilderCase())
                .Prepend(_ => new BinaryLocalDateSerializerBuilderCase())
            ));
    }
    public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
    {
        _serializer ??= await _serializerBuilder.Build<T>(_subject, AutomaticRegistrationBehavior.Always);
        return _serializer.Serialize(data, context);
    }
}