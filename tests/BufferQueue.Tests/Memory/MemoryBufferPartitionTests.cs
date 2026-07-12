// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferPartitionTests
{
    [Fact]
    public void New_Partition_Has_Initial_Capacity_And_No_Items()
    {
        var partition = new MemoryBufferPartition<int>(10, 4);

        Assert.Equal(10, partition.PartitionId);
        Assert.Equal(4UL, partition.Capacity);
        Assert.Equal(0UL, partition.Count);
        Assert.False(partition.TryPull("TestGroup", 1, out var items));
        Assert.Null(items);
        Assert.Single(GetSegments(partition));
    }

    [Fact]
    public void Commit_Unknown_Group_Throws()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        var exception = Assert.Throws<InvalidOperationException>(() => partition.Commit("UnknownGroup"));
        Assert.Equal("Specified group name not found.", exception.Message);
    }

    [Fact]
    public void Enqueue_And_TryPull()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        for (var i = 0; i < 12; i++)
        {
            partition.Enqueue(i);
        }

        Assert.True(partition.TryPull("TestGroup", 4, out var items));
        Assert.Equal(new[] { 0, 1, 2, 3 }, items);
        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 3, out items));
        Assert.Equal(new[] { 4, 5, 6 }, items);
        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 2, out items));
        Assert.Equal(new[] { 7, 8 }, items);
        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 4, out items));
        Assert.Equal(new[] { 9, 10, 11 }, items);
        partition.Commit("TestGroup");

        Assert.False(partition.TryPull("TestGroup", 2, out _));

        partition.Enqueue(12);

        Assert.True(partition.TryPull("TestGroup", 3, out items));
        Assert.Equal(new[] { 12 }, items);
    }

    [Fact]
    public void Enqueue_Updates_Count_And_Capacity_Across_Segments()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        Assert.Equal(2UL, partition.Capacity);
        Assert.Equal(0UL, partition.Count);

        partition.Enqueue(0);
        partition.Enqueue(1);

        Assert.Equal(2UL, partition.Capacity);
        Assert.Equal(2UL, partition.Count);

        partition.Enqueue(2);

        Assert.Equal(4UL, partition.Capacity);
        Assert.Equal(3UL, partition.Count);
        Assert.Equal(2, GetSegments(partition).Count);
    }

    [Fact]
    public void TryPull_Returns_Partial_Batch_When_BatchSize_Exceeds_Available_Items()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);

        partition.Enqueue(0);
        partition.Enqueue(1);

        Assert.True(partition.TryPull("TestGroup", 5, out var items));
        Assert.Equal(new[] { 0, 1 }, items);

        var collection = Assert.IsAssignableFrom<ICollection<int>>(items);
        Assert.Equal(2, collection.Count);
        Assert.True(collection.IsReadOnly);
        Assert.True(collection.Contains(1));

        var copy = new[] { -1, -1, -1, -1 };
        collection.CopyTo(copy, 1);
        Assert.Equal(new[] { -1, 0, 1, -1 }, copy);

        Assert.Throws<NotSupportedException>(() => collection.Add(3));
        Assert.Throws<NotSupportedException>(() => collection.Remove(0));
        Assert.Throws<NotSupportedException>(() => collection.Clear());
    }

    [Fact]
    public void TryPull_Single_Item_Batch_Exposes_ReadOnly_Collection()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        partition.Enqueue(42);

        Assert.True(partition.TryPull("TestGroup", 1, out var items));

        var list = Assert.IsAssignableFrom<IReadOnlyList<int>>(items);
        Assert.Single(list);
        Assert.Equal(42, list[0]);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[1]);

        var collection = Assert.IsAssignableFrom<ICollection<int>>(items);
        Assert.True(collection.IsReadOnly);
        Assert.True(collection.Contains(42));

        var copy = new[] { -1, -1 };
        collection.CopyTo(copy, 1);
        Assert.Equal(new[] { -1, 42 }, copy);

        Assert.Throws<NotSupportedException>(() => collection.Add(43));
        Assert.Throws<NotSupportedException>(() => collection.Remove(42));
        Assert.Throws<NotSupportedException>(() => collection.Clear());
    }

    [Fact]
    public void TryPull_Zero_BatchSize_Returns_False_And_Does_Not_Advance_Reader()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        partition.Enqueue(0);

        Assert.False(partition.TryPull("TestGroup", 0, out var items));
        Assert.Null(items);

        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 1, out items));
        Assert.Equal(new[] { 0 }, items);
    }

    [Fact]
    public void Repeatable_Pull_If_Not_Commit()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        for (var i = 0; i < 11; i++)
        {
            partition.Enqueue(i);
        }

        Assert.True(partition.TryPull("TestGroup", 4, out var items));
        Assert.Equal(new[] { 0, 1, 2, 3 }, items);

        Assert.True(partition.TryPull("TestGroup", 3, out items));
        Assert.Equal(new[] { 0, 1, 2 }, items);

        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 3, out items));
        Assert.Equal(new[] { 3, 4, 5 }, items);

        Assert.True(partition.TryPull("TestGroup", 5, out items));
        Assert.Equal(new[] { 3, 4, 5, 6, 7 }, items);

        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 6, out items));
        Assert.Equal(new[] { 8, 9, 10 }, items);

        Assert.True(partition.TryPull("TestGroup", 3, out items));
        Assert.Equal(new[] { 8, 9, 10 }, items);

        partition.Commit("TestGroup");

        Assert.False(partition.TryPull("TestGroup", 2, out _));
    }

    [Fact]
    public void Different_Groups_Read_Independently()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        for (var i = 0; i < 4; i++)
        {
            partition.Enqueue(i);
        }

        Assert.True(partition.TryPull("Group1", 3, out var group1Items));
        Assert.Equal(new[] { 0, 1, 2 }, group1Items);
        partition.Commit("Group1");

        Assert.True(partition.TryPull("Group2", 2, out var group2Items));
        Assert.Equal(new[] { 0, 1 }, group2Items);
        partition.Commit("Group2");

        Assert.True(partition.TryPull("Group1", 2, out group1Items));
        Assert.Equal(new[] { 3 }, group1Items);

        Assert.True(partition.TryPull("Group2", 3, out group2Items));
        Assert.Equal(new[] { 2, 3 }, group2Items);
    }

    [Fact]
    public void Segment_Will_Be_Recycled_If_All_Consumers_Consumed_Single_Group()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);

        for (var i = 0; i < 9; i++)
        {
            partition.Enqueue(i);
        }

        var segments1 = GetSegments(partition);

        for (var i = 0; i < 2; i++)
        {
            Assert.True(partition.TryPull("TestGroup", 3, out var items));
            Assert.Equal(new[] { i * 3, i * 3 + 1, i * 3 + 2 }, items);
            partition.Commit("TestGroup");
        }

        partition.Enqueue(9);

        for (var i = 0; i < 4; i++)
        {
            Assert.True(partition.TryPull("TestGroup", 1, out var items));
            Assert.Equal(i + 6, items.Single());
            partition.Commit("TestGroup");
        }

        var segments2 = GetSegments(partition);

        Assert.True(GetSlots(segments1[1]) == GetSlots(segments2[1]));
        Assert.True(GetSlots(segments1[2]) == GetSlots(segments2[0]));
    }

    [Fact]
    public void Segment_Will_Be_Recycled_If_All_Consumers_Consumed_MultipleGroup()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);

        for (var i = 0; i < 9; i++)
        {
            partition.Enqueue(i);
        }

        var segments1 = GetSegments(partition);

        for (var i = 0; i < 3; i++)
        {
            Assert.True(partition.TryPull("TestGroup1", 1, out _));
            partition.Commit("TestGroup1");
        }

        for (var i = 0; i < 2; i++)
        {
            Assert.True(partition.TryPull("TestGroup2", 3, out _));
            partition.Commit("TestGroup2");
        }

        partition.Enqueue(9);

        var segments2 = GetSegments(partition);

        Assert.True(GetSlots(segments1[1]) == GetSlots(segments2[0]));
        Assert.True(GetSlots(segments1[0]) == GetSlots(segments2[2]));
    }

    [Fact]
    public void Segment_Will_Not_Be_Recycled_If_Not_All_Consumers_Consumed_MultipleGroup()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);

        for (var i = 0; i < 6; i++)
        {
            partition.Enqueue(i);
        }

        var segments1 = GetSegments(partition);

        for (var i = 0; i < 3; i++)
        {
            Assert.True(partition.TryPull("TestGroup1", 1, out _));
            partition.Commit("TestGroup1");
        }

        for (var i = 0; i < 2; i++)
        {
            Assert.True(partition.TryPull("TestGroup2", 1, out _));
            partition.Commit("TestGroup2");
        }

        partition.Enqueue(7);

        var segments2 = GetSegments(partition);

        Assert.Equal(GetSlots(segments1[0]), GetSlots(segments2[0]));
        Assert.Equal(GetSlots(segments1[1]), GetSlots(segments2[1]));
    }

    [Fact]
    public void Segment_Will_Not_Be_Recycled_If_Consumer_Pulled_But_Not_Committed()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);

        for (var i = 0; i < 6; i++)
        {
            partition.Enqueue(i);
        }

        var segments1 = GetSegments(partition);

        Assert.True(partition.TryPull("TestGroup", 3, out var items));
        Assert.Equal(new[] { 0, 1, 2 }, items);

        partition.Enqueue(6);

        var segments2 = GetSegments(partition);

        Assert.Equal(GetSlots(segments1[0]), GetSlots(segments2[0]));
        Assert.Equal(GetSlots(segments1[1]), GetSlots(segments2[1]));
        Assert.Equal(3, segments2.Count);
    }

    [Fact]
    public void Segment_Recycling_Does_Not_Change_Visible_Read_Order()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        for (var i = 0; i < 6; i++)
        {
            partition.Enqueue(i);
        }

        for (var i = 0; i < 2; i++)
        {
            Assert.True(partition.TryPull("TestGroup", 2, out var items));
            Assert.Equal(new[] { i * 2, i * 2 + 1 }, items);
            partition.Commit("TestGroup");
        }

        partition.Enqueue(6);
        partition.Enqueue(7);

        Assert.True(partition.TryPull("TestGroup", 10, out var remainingItems));
        Assert.Equal(new[] { 4, 5, 6, 7 }, remainingItems);
    }

    private List<MemoryBufferSegment<int>> GetSegments(MemoryBufferPartition<int> partition)
    {
        var head = typeof(MemoryBufferPartition<>)
            .MakeGenericType(typeof(int))
            .GetField("_head", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(partition) as MemoryBufferSegment<int>;

        var segments = new List<MemoryBufferSegment<int>>();
        var segment = head;
        while (segment != null)
        {
            segments.Add(segment);
            segment = segment.NextSegment;
        }

        return segments;
    }

    private T[]? GetSlots<T>(MemoryBufferSegment<T> segment)
    {
        return typeof(MemoryBufferSegment<>)
            .MakeGenericType(typeof(T))
            .GetField("_slots", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(segment) as T[];
    }
}
