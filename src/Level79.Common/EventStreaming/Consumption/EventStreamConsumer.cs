using Chr.Avro.Confluent;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Level79.Common.EventStreaming.Consumption.Deserialization;

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

        var deserializerBuilderCases = new Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>[]
        {
            builder => new ReflectionBinaryRecordDeserializerBuilderCase(builder),
            _ => new InstantTimestampDeserializerBuilderCase(),
            _ => new LocalDateDeserializerBuilderCase(),
        };
        
        var deserializerBuilder = new BinaryDeserializerBuilder(
            deserializerBuilderCases.Concat(BinaryDeserializerBuilder.CreateDefaultCaseBuilders())
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