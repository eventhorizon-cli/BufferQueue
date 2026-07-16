// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace BufferQueue;

internal abstract class BufferQueue<TItem>(
    string topicName,
    IBufferPartition<TItem>[] partitions,
    IBufferProducer<TItem> producer)
    : IBufferQueue<TItem>
{
    private readonly object _consumersLock = new();
    private readonly Dictionary<string, List<BufferPullConsumer<TItem>>> _consumers = new();

    public string TopicName { get; } = topicName;

    public IBufferProducer<TItem> GetProducer() => producer;

    public IBufferPullConsumer<TItem> CreateConsumer(BufferPullConsumerOptions options)
    {
        var consumers = CreateConsumers(options, 1);
        return consumers.Single();
    }

    public IEnumerable<IBufferPullConsumer<TItem>> CreateConsumers(BufferPullConsumerOptions options, int consumerNumber)
    {
        ValidateConsumerOptions(options, consumerNumber);

        var groupName = options.GroupName;
        lock (_consumersLock)
        {
            if (_consumers.ContainsKey(groupName))
            {
                throw new InvalidOperationException($"The consumer group '{groupName}' already exists.");
            }

            var consumers = new List<BufferPullConsumer<TItem>>();
            for (var i = 0; i < consumerNumber; i++)
            {
                consumers.Add(new BufferPullConsumer<TItem>(options));
            }

            AssignPartitions(consumers);
            _consumers.Add(groupName, consumers);

            return consumers;
        }
    }

    private void ValidateConsumerOptions(BufferPullConsumerOptions options, int consumerNumber)
    {
        if (consumerNumber < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerNumber),
                "The number of consumers must be greater than 0.");
        }

        if (consumerNumber > partitions.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerNumber),
                "The number of consumers cannot be greater than the number of partitions.");
        }

        if (string.IsNullOrWhiteSpace(options.GroupName))
        {
            throw new ArgumentException("The group name cannot be null or empty.", nameof(options.GroupName));
        }

        if (options.BatchSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BatchSize),
                "The batch size must be greater than 0.");
        }
    }

    private void AssignPartitions(List<BufferPullConsumer<TItem>> consumers)
    {
        var consumerNumber = consumers.Count;
        var partitionsPerConsumer = partitions.Length / consumerNumber;
        var partitionsRemainder = partitions.Length % consumerNumber;
        var partitionStartIndex = 0;
        foreach (var consumer in consumers)
        {
            var extraPartitions = partitionsRemainder > 0 ? 1 : 0;
            var partitionEndIndex = partitionStartIndex
                                    + partitionsPerConsumer
                                    + extraPartitions;
            var partitions1 = partitions[partitionStartIndex..partitionEndIndex];
            consumer.AssignPartitions(partitions1);

            partitionStartIndex = partitionEndIndex;

            if (partitionsRemainder > 0)
            {
                partitionsRemainder--;
            }
        }
    }
}
