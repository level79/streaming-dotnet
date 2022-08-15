using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Level79.Common.EventStreaming.Production;

public class AsyncSchemaRegistryGenericEventSerializer<T> : IAsyncSerializer<T> where T : IEvent
{
    private readonly CachedSchemaRegistryClient _schemaRegistry;
    private readonly Dictionary<Type, object?> _eventSerializerCache = new();

    public AsyncSchemaRegistryGenericEventSerializer(CachedSchemaRegistryClient schemaRegistry)
    {
        _schemaRegistry = schemaRegistry;
    }

    public Task<byte[]> SerializeAsync(T data, SerializationContext context)
    {
        return SerializeWithEventSerializer((dynamic) data, context);
    }

    private Task<byte[]> SerializeWithEventSerializer<TEvent>(TEvent data, SerializationContext context) where TEvent : IEvent
    {
        AsyncSchemaRegistryEventSerializer<TEvent>? eventSerializer;
        
        if (_eventSerializerCache.ContainsKey(typeof(TEvent)))
        {
            eventSerializer = _eventSerializerCache[typeof(TEvent)] as AsyncSchemaRegistryEventSerializer<TEvent>;
        }
        else
        {
            eventSerializer = new AsyncSchemaRegistryEventSerializer<TEvent>(_schemaRegistry);
            _eventSerializerCache.Add(typeof(TEvent), eventSerializer);    
        }
        return eventSerializer?.SerializeAsync(data, context) ?? Task.FromResult(Array.Empty<byte>());
    }
}