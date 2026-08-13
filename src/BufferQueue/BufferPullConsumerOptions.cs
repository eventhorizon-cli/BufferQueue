namespace BufferQueue;

public class BufferPullConsumerOptions
{
    public required string TopicName { get; init; }

    public required string GroupName { get; init; }

    public bool AutoCommit { get; init; }

    public int BatchSize { get; init; } = 100;
}
