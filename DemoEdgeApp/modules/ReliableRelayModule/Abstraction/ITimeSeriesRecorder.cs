namespace ReliableRelayModule.Abstraction
{
    using System.Threading.Tasks;

    public interface ITimeSeriesRecorder
    {
        Task InitializeAsync();
        Task RecordMessageAsync(string telemetryJson);
    }
}
