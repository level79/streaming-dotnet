using System;
using Level79.Common.EventStreaming;

namespace Level79.Common.Test.Reflection;

public class EventInAnotherAssembly : IEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
}