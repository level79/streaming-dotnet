using Chr.Avro.Abstract;
using Chr.Avro.Confluent;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Level79.Common.EventStreaming.Production;

public class AsyncSchemaRegistryEventSerializer<T> : IAsyncSerializer<T> where T : IEvent
{
    private readonly string _subject;
    private readonly SchemaRegistrySerializerBuilder _serializerBuilder;
    private ISerializer<T>? _serializer;

    public AsyncSchemaRegistryEventSerializer(ISchemaRegistryClient schemaRegistry)
    {
        _subject = typeof(T).FullName?.ToLowerInvariant() ?? "UnknownSubject";
        
        var schemaBuilder = new SchemaBuilder(
            SchemaBuilder.CreateDefaultCaseBuilders()
            .Prepend(builder => new InstantSchemaBuilderCase(TemporalBehavior.EpochMicroseconds))
        );

        _serializerBuilder = new SchemaRegistrySerializerBuilder(
            schemaRegistry,
            schemaBuilder,
            serializerBuilder: new BinarySerializerBuilder(
                BinarySerializerBuilder.CreateDefaultCaseBuilders()
                .Prepend(builder => new BinaryInstantSerializerBuilderCase())
            ));
    }
    public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
    {
        _serializer ??= await _serializerBuilder.Build<T>(_subject, AutomaticRegistrationBehavior.Always);
        return _serializer.Serialize(data, context);
    }
}