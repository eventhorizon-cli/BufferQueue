// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using BufferQueue.Benchmarks;

var config = ManualConfig
    .Create(DefaultConfig.Instance)
    .AddDiagnoser(MemoryDiagnoser.Default);

var allBenchmarks = new[]
{
    typeof(MemoryBufferQueueProduceBenchmark),
    typeof(MemoryBufferQueueConsumeBenchmark),
};

new BenchmarkSwitcher(allBenchmarks).Run(args, config);
