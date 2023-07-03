using System;

namespace Azure.Samples.ReliableEdgeRelay.Types
{
    /// <summary>
    /// The BackfillRequest is the request sent to the device to fill a single data gap
    /// </summary>
    public class BackfillRequest
    {
        public string DeviceID { get; set; }
        public string BackfillStartTime { get; set; }
        public string BackfillEndTime { get; set; }
        
        public DateTime CreatedAt { get; set; }
    }
}
