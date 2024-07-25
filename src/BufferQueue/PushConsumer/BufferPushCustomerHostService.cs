// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BufferQueue.PushConsumer;

internal class BufferPushCustomerHostService(
    IBufferQueue bufferQueue,
    IEnumerable<BufferPushConsumerDescription> consumerDescriptions,
    IServiceProvider serviceProvider,
    ILogger<BufferPushCustomerHostService> logger)
    : IHostedService
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var token = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token)
            .Token;

        var startPullConsumersMethod = typeof(BufferPushCustomerHostService)
            .GetMethod(nameof(StartPullConsumers), BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetGenericMethodDefinition();

        foreach (var description in consumerDescriptions)
        {
            var itemType = description.ServiceDescriptor.ServiceType
                .GetInterfaces()
                .First(x => x.IsGenericType && x.IsAssignableTo(typeof(IBufferPushConsumer)))
                .GetGenericArguments()[0];

            var method = startPullConsumersMethod.MakeGenericMethod(itemType);

            method.Invoke(this, [description, token]);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        return Task.CompletedTask;
    }

    private void StartPullConsumers<T>(
        BufferPushConsumerDescription consumerDescription,
        CancellationToken cancellationToken)
    {
        var (options, serviceDescriptor, concurrency) = consumerDescription;

        var customers = bufferQueue.CreatePullConsumers<T>(options, concurrency);

        foreach (var customer in customers)
        {
            _ = ConsumeAsync(customer, serviceDescriptor, cancellationToken);
        }
    }

    private async Task ConsumeAsync<T>(
        IBufferPullConsumer<T> pullConsumer,
        ServiceDescriptor serviceDescriptor,
        CancellationToken cancellationToken)
    {
        object? pushConsumerObj = null;
        if (serviceDescriptor.Lifetime == ServiceLifetime.Singleton)
        {
            pushConsumerObj = serviceProvider.GetRequiredService(serviceDescriptor.ServiceType);
        }

        IServiceScope? scope = null;

        await foreach (var buffer in pullConsumer.ConsumeAsync(cancellationToken))
        {
            try
            {
                if (pushConsumerObj is null)
                {
                    scope = serviceProvider.CreateScope();
                    pushConsumerObj = scope.ServiceProvider.GetRequiredService(serviceDescriptor.ServiceType);
                }

                var consumeTask = pushConsumerObj switch
                {
                    IBufferManualCommitPushConsumer<T> manualCommitConsumer =>
                        manualCommitConsumer.ConsumeAsync(buffer, pullConsumer, cancellationToken),
                    IBufferAutoCommitPushConsumer<T> autoCommitConsumer =>
                        autoCommitConsumer.ConsumeAsync(buffer, cancellationToken),
                    _ => throw new InvalidOperationException(
                        $"The service {serviceDescriptor.ServiceType} does not implement {nameof(IBufferManualCommitPushConsumer<T>)} or {nameof(IBufferAutoCommitPushConsumer<T>)}")
                };

                await consumeTask;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while consuming from the buffer.");
            }
            finally
            {
                scope?.Dispose();
            }
        }
    }
}
