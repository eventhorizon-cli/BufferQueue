using BufferQueue;
using BufferQueue.Memory;
using WebAPI;
using WebApp;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Configure the BufferQueue
builder.Services.AddBufferQueue(options =>
{
    options.UseMemory(bufferOptions =>
        {
            bufferOptions.AddTopic<Foo>("topic-foo1", partitionNumber: 6);
            bufferOptions.AddTopic<Foo>("topic-foo2", partitionNumber: 4);
            bufferOptions.AddTopic<Bar>("topic-bar", partitionNumber: 8);
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

builder.Services.AddHostedService<Foo1PullConsumerHostService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapControllers();

app.Run();
