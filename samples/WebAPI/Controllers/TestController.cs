using BufferQueue;
using Microsoft.AspNetCore.Mvc;
using WebApp;

namespace WebAPI.Controllers;

[ApiController]
[Route("/api/[controller]")]
public class TestController(IBufferQueue bufferQueue) : ControllerBase
{
    [HttpPost("foo1")]
    public async Task<IActionResult> PostFoo1([FromBody] Foo foo)
    {
        var producer = bufferQueue.GetProducer<Foo>("topic-foo1");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("foo2")]
    public async Task<IActionResult> PostFoo2([FromBody] Foo foo)
    {
        var producer = bufferQueue.GetProducer<Foo>("topic-foo2");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("bar")]
    public async Task<IActionResult> PostBar([FromBody] Bar bar)
    {
        var producer = bufferQueue.GetProducer<Bar>("topic-bar");
        await producer.ProduceAsync(bar);
        // TryProduceAsync can be used if you want to check if the item was produced successfully
        // bool success = await producer.TryProduceAsync(bar);
        return Ok();
    }
}
