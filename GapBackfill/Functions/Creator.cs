using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Queue;

using Newtonsoft.Json;

namespace Azure.Samples.ReliableEdgeRelay.Functions
{
    /// <summary>
    /// An Azure Function that receives detected orphan messages, checks the received data log for data gaps and 
    /// starts the data gap back-fill process. It will enqueue a data gap message in the output queue that will be
    /// processed.
    /// </summary>    
    public static class Creator
    {
        [FunctionName("CreateBackfillRequest")]
        public static async Task RunAsync(
            [EventHubTrigger("%DetectionEventHubName%", Connection = "EventHubConnectionString")] EventData[] inputEvents,
            [Queue("%StorageQueueName%", Connection = "StorageConnectionString")] CloudQueue outputQueue,
            ILogger logger,
            ExecutionContext context)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in inputEvents)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    logger.LogInformation($"{context.FunctionName}: {messageBody}");

                    // AzFunctions will append multiple messages in the same event
                    var messageBodies = messageBody.Split(System.Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

                    foreach (string message in messageBodies)
                    {
                        var orphanMessage = JsonConvert.DeserializeObject<Types.OrphanMessage>(message);

                        var dataGaps = await Helpers.SqlHelpers.GetDataGaps(orphanMessage, logger);

                        foreach (var dataGap in dataGaps)
                        {
                            logger.LogInformation($"{context.FunctionName}: Enqueueing data gap message..");
                            await outputQueue.AddMessageAsync(
                                new CloudQueueMessage(
                                    JsonConvert.SerializeObject(dataGap)),
                                    TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DataGapsTTLSeconds"))),
                                    TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("InitialDetectionInvincibleSeconds"))),
                                    null,
                                    null);
                        }
                        await Task.Yield();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            // All input events will be processed.
            // We will rethrow the generated processing exception(s)
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                ExceptionDispatchInfo.Capture(exceptions.Single()).Throw();
            
        }
    }
}
