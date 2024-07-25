namespace WebApp;

public class Foo
{
    public int Id { get; set; }

    public string? Value { get; set; }

    public override string ToString()
    {
        return $"Id: {Id}, Value: {Value}";
    }
}
