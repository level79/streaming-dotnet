using System;
using System.Threading;
using System.Threading.Tasks;
using Level79.Common.EventStreaming.Consumption;
using Level79.Common.EventStreaming.Production;
using NodaTime;
using Shouldly;
using Xunit;

namespace Level79.Common.Test.EventStreaming;

public class GivenAnEventWithNodaTimeTimestamps_ThenItCanBeProducedAndConsumed : IntegrationTest
{
    private const string Topic = "testtimestamp";
    private readonly EventStreamProducer _eventStreamProducer;
    private readonly EventStreamConsumer _eventStreamConsumer;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public GivenAnEventWithNodaTimeTimestamps_ThenItCanBeProducedAndConsumed()
    {
        _eventStreamProducer = new EventStreamProducer();
        _eventStreamConsumer = new EventStreamConsumer();
    }

    public override async Task Integration()
    {
        await _eventStreamConsumer.Subscribe(Topic);
        await base.Integration();
    }
    
    public override async Task Performance()
    {
        await _eventStreamConsumer.Subscribe(Topic);
        await base.Performance();
    }

    protected override async Task Execute(CancellationToken cancellationToken)
    {
        var @event = new TestTimestampEvent()
        {
            EventId = Guid.NewGuid(),
            Timestamp = DateTime.Now,
            TestTimestamp = SystemClock.Instance.GetCurrentInstant()
        };

        await _eventStreamProducer.ProduceAsync(Topic, @event, _cancellationTokenSource.Token);

        var consumedEvent = _eventStreamConsumer.GetNext(cancellationToken);
        consumedEvent.Commit();
        var result = consumedEvent.Event as TestTimestampEvent;
        result.ShouldNotBeNull();
        result.EventId.ShouldBe(@event.EventId);
        result.TestTimestamp.ShouldBe(@event.TestTimestamp);
    }
}