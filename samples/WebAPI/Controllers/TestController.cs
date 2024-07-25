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
        var producer = bufferQueue.CreateProducer<Foo>("topic-foo1");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("foo2")]
    public async Task<IActionResult> PostFoo2([FromBody] Foo foo)
    {
        var producer = bufferQueue.CreateProducer<Foo>("topic-foo2");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("bar")]
    public async Task<IActionResult> PostBar([FromBody] Bar bar)
    {
        var producer = bufferQueue.CreateProducer<Bar>("topic-bar");
        await producer.ProduceAsync(bar);
        return Ok();
    }
}
