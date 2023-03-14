
using System;

namespace Azure.Samples.ReliableEdgeRelay.Types
{
    /// <summary>
    /// A DataGap is a detected window of missing data in the incoming messages log table
    /// </summary>
    public class DataGap
    {
        public DateTime StartWindow { get; set; }
        public DateTime EndWindow { get; set; }
        public string BatchId { get; set; }
        public int GapInSeconds { get; set; }
    }
}
