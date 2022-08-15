using System.Runtime.Serialization;
using Avro.Specific;

namespace Level79.Common.EventStreaming;

public class HeartBeatEvent : IEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
}

public class AnotherEvent : IEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
}