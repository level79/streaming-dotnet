using System;
using Level79.Common.EventStreaming;
using NodaTime;

namespace Level79.Common.Test.EventStreaming;

public class TestTimestampEvent : IEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
    public Instant TestTimestamp { get; set; }
}