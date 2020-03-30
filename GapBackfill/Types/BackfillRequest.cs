
using System;

namespace Azure.Samples.ReliableEdgeRelay.Types
{
    /// <summary>
    /// The BackfillRequest is the request sent to the device to fill a single data gap
    /// </summary>
    internal class BackfillRequest
    {
        public DateTime StartWindow { get; set; }
        public DateTime EndWindow { get; set; }
        public string BatchId { get; set; }
        public DateTime Created { get; set; }
    }
}