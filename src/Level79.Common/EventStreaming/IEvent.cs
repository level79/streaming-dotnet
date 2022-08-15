namespace Level79.Common.EventStreaming;

public interface IEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
}