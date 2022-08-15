using Chr.Avro.Confluent;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

namespace Level79.Common.EventStreaming.Consumption;

public class EventStreamConsumer : IDisposable
{
    private readonly IConsumer<string, IEvent> _consumer;

    public EventStreamConsumer()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081",
        };
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            EnableAutoCommit = false,
            AutoCommitIntervalMs = 0,
            GroupId = $"level79-{Guid.NewGuid()}",
        };
        
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        var deserializerBuilder = new BinaryDeserializerBuilder(
            BinaryDeserializerBuilder.CreateDefaultCaseBuilders()
                .Prepend(builder => new InstantTimestampDeserializerBuilderCase())
                .Prepend(builder => new ReflectionBinaryRecordDeserializerBuilderCase(builder))
        );

        _consumer = new ConsumerBuilder<string, IEvent>(consumerConfig)
            .SetValueDeserializer(new AsyncSchemaRegistryDeserializer<IEvent>(
                schemaRegistry,
                deserializerBuilder).AsSyncOverAsync())
            .Build();
    }

    public async Task Subscribe(params string[] topics)
    {
        _consumer.Subscribe(topics);
        while (_consumer.Assignment.Count < 1)
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

    public CommittableEvent GetNext(CancellationToken cancellationToken)
    {
        var consumeResult = _consumer.Consume(cancellationToken);
        return new CommittableEvent(_consumer, consumeResult);
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }
}