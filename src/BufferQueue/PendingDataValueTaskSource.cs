using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace BufferQueue;

internal sealed class PendingDataValueTaskSource<T> : IValueTaskSource<T>
{
    private ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = true };

    private ValueTask<T> _valueTask;
    public ValueTask<T> ValueTask => _valueTask;

    public void Reset()
    {
        _core.Reset();
        _valueTask = new(this, _core.Version);
    }

    public void SetResult(T result)
    {
        _core.SetResult(result);
    }

    public T GetResult(short token) => _core.GetResult(token);

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}
