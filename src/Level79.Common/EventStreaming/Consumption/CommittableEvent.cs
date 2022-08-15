using Confluent.Kafka;

namespace Level79.Common.EventStreaming.Consumption;

public class CommittableEvent
{
    private readonly IConsumer<string, IEvent> _consumer;
    private readonly ConsumeResult<string, IEvent> _consumeResult;

    public CommittableEvent(IConsumer<string, IEvent> consumer, ConsumeResult<string, IEvent> consumeResult)
    {
        _consumer = consumer;
        _consumeResult = consumeResult;
    }

    public IEvent Event => _consumeResult.Message.Value;

    public void Commit()
    {
        _consumer.Commit(_consumeResult);
    }
}