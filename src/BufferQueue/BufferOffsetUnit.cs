namespace BufferQueue;

/// <summary>The unit of an offset lag observation.</summary>
public enum BufferOffsetUnit
{
    /// <summary>Logical item positions in Memory storage.</summary>
    Items = 0,

    /// <summary>Byte positions in MemoryMappedFile storage, including record framing and segment padding.</summary>
    Bytes = 1
}
