using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.Tests.PushConsumer;

public sealed class BufferPushCustomerRegistrationTests
{
    [Fact]
    public void AddPushCustomers_Should_Not_Register_Abstract_Consumers()
    {
        var services = new ServiceCollection();

        new BufferOptionsBuilder(services)
            .AddPushCustomers(typeof(BufferPushCustomerRegistrationTests).Assembly);

        Assert.DoesNotContain(
            services,
            descriptor => descriptor.ServiceType == typeof(AbstractPushConsumer));
    }

    [BufferPushCustomer("unused-topic", "unused-group", 1, ServiceLifetime.Singleton, 1)]
    public abstract class AbstractPushConsumer : IBufferAutoCommitPushConsumer<int>
    {
        public Task ConsumeAsync(IEnumerable<int> buffer, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}
