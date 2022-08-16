using System;
using System.Threading;
using System.Threading.Tasks;
using Level79.Common.EventStreaming.Consumption;
using Level79.Common.EventStreaming.Production;
using NodaTime;
using Shouldly;

namespace Level79.Common.Test.EventStreaming;

public class GivenAnEventWithNodatimeLocalDates_ThenItCanBeProducedAndConsumed : IntegrationTest
{
    private const string Topic = "testlocaldate";

    private readonly EventStreamProducer _eventStreamProducer;
    private readonly EventStreamConsumer _eventStreamConsumer;

    public GivenAnEventWithNodatimeLocalDates_ThenItCanBeProducedAndConsumed()
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
        var now = SystemClock.Instance.GetCurrentInstant();
        
        var @event = new TestLocalDateEvent()
        {
            EventId = Guid.NewGuid(),
            Timestamp = DateTime.Now,
            TestDate = now.InUtc().Date
        };

        await _eventStreamProducer.ProduceAsync(Topic, @event, cancellationToken);

        var consumedEvent = _eventStreamConsumer.GetNext(cancellationToken);
        consumedEvent.Commit();
        var result = consumedEvent.Event as TestLocalDateEvent;
        result.ShouldNotBeNull();
        result.EventId.ShouldBe(@event.EventId);
        result.TestDate.ShouldBe(@event.TestDate);
    }
}