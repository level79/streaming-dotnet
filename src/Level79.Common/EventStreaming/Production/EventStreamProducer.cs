using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

namespace Level79.Common.EventStreaming.Production;

public class EventStreamProducer : IDisposable
{
    private readonly CachedSchemaRegistryClient _schemaRegistry;
    private readonly IProducer<string, IEvent> _producer;

    public EventStreamProducer()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081",
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        _schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        var producerBuilder = new ProducerBuilder<string, IEvent>(producerConfig);
        var valueSerializer = new AsyncSchemaRegistryGenericEventSerializer<IEvent>(_schemaRegistry);
        producerBuilder
            .SetValueSerializer(valueSerializer);
        _producer = producerBuilder.Build();
    }

    public async Task ProduceAsync(string topic, IEvent @event, CancellationToken cancellationToken)
    {
        var message = new Message<string, IEvent>
        {
            Key = Guid.NewGuid().ToString(),
            Value = @event
        };
        var deliveryResult = await _producer.ProduceAsync(topic, message, cancellationToken);
        if (deliveryResult.Status != PersistenceStatus.Persisted) throw new Exception("Could not delivery message");
    }

    public void Dispose()
    {
        _producer.Dispose();
        _schemaRegistry.Dispose();
    }
}