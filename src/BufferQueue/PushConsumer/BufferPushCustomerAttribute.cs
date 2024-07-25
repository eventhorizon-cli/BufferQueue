// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.PushConsumer;

[AttributeUsage(AttributeTargets.Class)]
public class BufferPushCustomerAttribute : Attribute
{
    public string TopicName { get; }

    public string GroupName { get; }

    public int BatchSize { get; }

    public ServiceLifetime ServiceLifetime { get; }

    public int Concurrency { get; }

    public BufferPushCustomerAttribute(
        string topicName,
        string groupName,
        int batchSize,
        ServiceLifetime serviceLifetime,
        int concurrency
    )
    {
        if (string.IsNullOrWhiteSpace(topicName))
        {
            throw new ArgumentNullException(nameof(topicName));
        }

        if (string.IsNullOrWhiteSpace(groupName))
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        if (batchSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize));
        }

        if (concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(concurrency));
        }

        TopicName = topicName;
        GroupName = groupName;
        BatchSize = batchSize;
        ServiceLifetime = serviceLifetime;
        Concurrency = concurrency;
    }
}
