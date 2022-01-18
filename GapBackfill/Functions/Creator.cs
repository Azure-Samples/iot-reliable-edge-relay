using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Storage.Queues;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
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
            [Queue("%StorageQueueName%", Connection = "StorageConnectionString")] QueueClient outputQueue,
            ILogger logger,
            ExecutionContext context)
        {
            var exceptions = new List<RequestFailedException>();

            foreach (EventData eventData in inputEvents)
            {
                try
                {
                    byte[] messageBody = eventData.EventBody.ToArray();

                    logger.LogInformation($"{context.FunctionName}: {messageBody}");

                    // AzFunctions will append multiple messages in the same event
                    var messageBodies = messageBody.ToString().Split(System.Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

                    foreach (string message in messageBodies)
                    {
                        var orphanMessage = JsonConvert.DeserializeObject<Types.OrphanMessage>(message);

                        var dataGaps = await Helpers.SqlHelpers.GetDataGaps(orphanMessage, logger);

                        foreach (var dataGap in dataGaps)
                        {
                            logger.LogInformation($"{context.FunctionName}: Enqueueing data gap message..");
                            await outputQueue.SendMessageAsync(
                                    JsonConvert.SerializeObject(dataGap),
                                    TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DataGapsTTLSeconds"))),
                                    TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("InitialDetectionInvincibleSeconds")))
                                    );
                        }
                        await Task.Yield();
                    }
                }
                catch (RequestFailedException e)
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
