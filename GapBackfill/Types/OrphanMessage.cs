
using System;

namespace Azure.Samples.ReliableEdgeRelay.Types
{
    /// <summary>
    /// An orthan message is a message with StartWindow = T1 for which no message was found with EndWindow = T1
    /// This is an indication of a data gap. 
    /// </summary>
    public class OrphanMessage
    {
        public DateTime StartWindow { get; set; }
        public DateTime EndWindow { get; set; }
        public string BatchId { get; set; }
    }
}
