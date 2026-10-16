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
    : IHostedService, IDisposable
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly object _consumerTasksLock = new();
    private readonly List<Task> _consumerTasks = [];
    private CancellationTokenSource? _linkedCancellationTokenSource;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var linkedCancellationTokenSource = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);
        _linkedCancellationTokenSource = linkedCancellationTokenSource;
        var token = linkedCancellationTokenSource.Token;

        var startPullConsumersMethod = typeof(BufferPushCustomerHostService)
            .GetMethod(nameof(StartPullConsumers), BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetGenericMethodDefinition();

        try
        {
            foreach (var description in consumerDescriptions)
            {
                var itemType = description.ServiceDescriptor.ServiceType
                    .GetInterfaces()
                    .First(x => x.IsGenericType && x.IsAssignableTo(typeof(IBufferPushConsumer)))
                    .GetGenericArguments()[0];

                var method = startPullConsumersMethod.MakeGenericMethod(itemType);
                method.Invoke(this, [description, token]);
            }
        }
        catch
        {
            _cancellationTokenSource.Cancel();
            linkedCancellationTokenSource.Dispose();
            _linkedCancellationTokenSource = null;
            throw;
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();

        Task[] consumerTasks;
        lock (_consumerTasksLock)
        {
            consumerTasks = [.. _consumerTasks];
        }

        var allConsumerTasks = Task.WhenAll(consumerTasks);
        try
        {
            await allConsumerTasks.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (allConsumerTasks.IsCompleted)
            {
                DisposeLinkedCancellationTokenSource();
            }
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        DisposeLinkedCancellationTokenSource();
        _cancellationTokenSource.Dispose();
    }

    private void StartPullConsumers<T>(
        BufferPushConsumerDescription consumerDescription,
        CancellationToken cancellationToken)
    {
        var (options, serviceDescriptor, concurrency) = consumerDescription;
        var customers = bufferQueue.CreatePullConsumers<T>(options, concurrency);

        foreach (var customer in customers)
        {
            var task = ConsumeAsync(customer, serviceDescriptor, cancellationToken);
            lock (_consumerTasksLock)
            {
                _consumerTasks.Add(task);
            }
        }
    }

    private async Task ConsumeAsync<T>(
        IBufferPullConsumer<T> pullConsumer,
        ServiceDescriptor serviceDescriptor,
        CancellationToken cancellationToken)
    {
        try
        {
            object? singletonPushConsumer = null;
            if (serviceDescriptor.Lifetime == ServiceLifetime.Singleton)
            {
                singletonPushConsumer = serviceProvider.GetRequiredService(serviceDescriptor.ServiceType);
            }

            await foreach (var buffer in pullConsumer.ConsumeAsync(cancellationToken))
            {
                try
                {
                    if (singletonPushConsumer is not null)
                    {
                        await ConsumeBufferAsync(
                            singletonPushConsumer,
                            serviceDescriptor.ServiceType,
                            buffer,
                            pullConsumer,
                            cancellationToken)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await using var scope = serviceProvider.CreateAsyncScope();
                        var pushConsumer = scope.ServiceProvider.GetRequiredService(serviceDescriptor.ServiceType);
                        await ConsumeBufferAsync(
                            pushConsumer,
                            serviceDescriptor.ServiceType,
                            buffer,
                            pullConsumer,
                            cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "An error occurred while consuming topic '{TopicName}' for group '{GroupName}'.",
                        pullConsumer.TopicName,
                        pullConsumer.GroupName);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "The push consumer loop failed for topic '{TopicName}' and group '{GroupName}'.",
                pullConsumer.TopicName,
                pullConsumer.GroupName);
        }
        finally
        {
            if (pullConsumer is BufferPullConsumer<T> bufferPullConsumer)
            {
                bufferPullConsumer.UnassignPartitions();
            }
        }
    }

    private void DisposeLinkedCancellationTokenSource()
    {
        var linkedCancellationTokenSource = Interlocked.Exchange(ref _linkedCancellationTokenSource, null);
        linkedCancellationTokenSource?.Dispose();
    }

    private static Task ConsumeBufferAsync<T>(
        object pushConsumer,
        Type serviceType,
        IEnumerable<T> buffer,
        IBufferPullConsumer<T> pullConsumer,
        CancellationToken cancellationToken) =>
        pushConsumer switch
        {
            IBufferManualCommitPushConsumer<T> manualCommitConsumer =>
                manualCommitConsumer.ConsumeAsync(buffer, pullConsumer, cancellationToken),
            IBufferAutoCommitPushConsumer<T> autoCommitConsumer =>
                autoCommitConsumer.ConsumeAsync(buffer, cancellationToken),
            _ => throw new InvalidOperationException(
                $"The service {serviceType} does not implement {nameof(IBufferManualCommitPushConsumer<T>)} or {nameof(IBufferAutoCommitPushConsumer<T>)}")
        };
}
