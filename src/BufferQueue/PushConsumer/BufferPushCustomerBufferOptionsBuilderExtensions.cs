// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue;

public static class BufferPushCustomerBufferOptionsBuilderExtensions
{
    public static BufferOptionsBuilder AddPushCustomers(
        this BufferOptionsBuilder builder,
        Assembly assembly)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (assembly is null)
        {
            throw new ArgumentNullException(nameof(assembly));
        }

        var consumerDescriptors = new List<BufferPushConsumerDescription>();

        foreach (var type in assembly.GetTypes())
        {
            if (type.IsClass == false)
            {
                continue;
            }

            var interfaceType = type.GetInterfaces()
                .FirstOrDefault(x => x.IsGenericType && x.IsAssignableTo(typeof(IBufferPushConsumer)));

            if (interfaceType is null)
            {
                continue;
            }

            var attribute = type.GetCustomAttribute<BufferPushCustomerAttribute>();

            if (attribute is null)
            {
                throw new InvalidOperationException(
                    $"The BufferConsumer {type.FullName} does not have a BufferCustomerAttribute applied to it.");
            }

            var autoCommit = interfaceType.GetGenericTypeDefinition() == typeof(IBufferAutoCommitPushConsumer<>);

            var options = new BufferPullConsumerOptions
            {
                TopicName = attribute.TopicName,
                GroupName = attribute.GroupName,
                AutoCommit = autoCommit,
                BatchSize = attribute.BatchSize,
            };

            var serviceDescriptor = new ServiceDescriptor(type, type, attribute.ServiceLifetime);
            builder.Services.Add(serviceDescriptor);

            var consumerDescriptor = new BufferPushConsumerDescription(
                options,
                serviceDescriptor,
                attribute.Concurrency);
            consumerDescriptors.Add(consumerDescriptor);
        }

        builder.Services.AddHostedService<BufferPushCustomerHostService>(sp =>
        {
            var hostService =
                ActivatorUtilities.CreateInstance<BufferPushCustomerHostService>(
                    sp,
                    consumerDescriptors.AsEnumerable());

            return hostService;
        });

        return builder;
    }
}
