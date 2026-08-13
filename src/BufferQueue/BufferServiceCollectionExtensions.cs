using System;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue;

public static class BufferQueueServiceCollectionExtensions
{
    public static IServiceCollection AddBufferQueue(
        this IServiceCollection services,
        Action<BufferOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddSingleton<IBufferQueue, BufferQueue>();
        configure(new BufferOptionsBuilder(services));
        return services;
    }
}
