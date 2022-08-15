using System;
using System.Threading;
using System.Threading.Tasks;
using Level79.Common.EventStreaming;
using Level79.Common.EventStreaming.Consumption;
using Level79.Common.EventStreaming.Production;
using Shouldly;

namespace Level79.Common.Test.EventStreaming;

public class GivenAnEventIsProducedToTheStream : IntegrationTest
{
    private readonly EventStreamProducer _eventStreamProducer;
    private readonly EventStreamConsumer _eventStreamConsumer;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public GivenAnEventIsProducedToTheStream()
    {
        _eventStreamProducer = new EventStreamProducer();
        _eventStreamConsumer = new EventStreamConsumer();
    }

    public override async Task Integration()
    {
        await _eventStreamConsumer.Subscribe("heartbeat");
        await base.Integration();
    }

    public override async Task Performance()
    {
        await _eventStreamConsumer.Subscribe("heartbeat");
        await base.Performance();
    }

    protected override async Task Execute(CancellationToken cancellationToken)
    {

        var heartBeatEvent = new HeartBeatEvent()
        {
            EventId = Guid.NewGuid(),
            Timestamp = DateTime.Now,
        };

        await _eventStreamProducer.ProduceAsync("heartbeat", heartBeatEvent, _cancellationTokenSource.Token);

        var consumedEvent = _eventStreamConsumer.GetNext(cancellationToken);
        consumedEvent.Commit();
        consumedEvent.Event.EventId.ShouldBe(heartBeatEvent.EventId);
    }
}