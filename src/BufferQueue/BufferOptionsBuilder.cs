using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue;

public class BufferOptionsBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = services;
}
