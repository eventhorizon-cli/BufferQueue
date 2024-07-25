// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.PushConsumer;

internal record BufferPushConsumerDescription(
    BufferPullConsumerOptions Options,
    ServiceDescriptor ServiceDescriptor,
    int Concurrency);
