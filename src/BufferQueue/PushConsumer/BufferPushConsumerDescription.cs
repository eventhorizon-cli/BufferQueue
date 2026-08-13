using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.PushConsumer;

internal record BufferPushConsumerDescription(
    BufferPullConsumerOptions Options,
    ServiceDescriptor ServiceDescriptor,
    int Concurrency);
