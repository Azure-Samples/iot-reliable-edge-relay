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
using Newtonsoft.Json.Linq;

namespace Azure.Samples.ReliableEdgeRelay.Functions
{
    /// An Azure Function that receives detected messages,
    /// invokes Direct Method call to the Edge Module for the data gap back-fill process. 
    /// It will enqueue a data gap message in the output queue for retry if the direct method call fails.  
    public static class BackfillRequest
    {
        [FunctionName("BackfillRequest")]
        public static async Task RunAsync(
            [EventHubTrigger("%DetectionEventHubName%", Connection = "EventHubConnectionString")] EventData[] inputEvents, 
            [Queue("%StorageQueueName%", Connection = "StorageConnectionString")] QueueClient outputQueue,
            ILogger logger,
            ExecutionContext context)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in inputEvents)
            {
                try
                {
                    string messageBody = eventData.EventBody.ToString();
                    var messageBodies = messageBody.ToString().Split(System.Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

                    foreach (string message in messageBodies)
                    {
                        JObject gapObject = JObject.Parse(message);
                        Types.BackfillRequest backfillRequest = new Types.BackfillRequest()
                        {
                            DeviceID = gapObject.GetValue("deviceID").ToString(),
                            BackfillStartTime = gapObject.GetValue("previousLastTsInBatch").ToString(),
                            BackfillEndTime = gapObject.GetValue("currfirstTsInBatch").ToString(),
                            CreatedAt = DateTime.UtcNow
                        };
                        logger.LogInformation($"backfillRequest: {JsonConvert.SerializeObject(backfillRequest)}");
                        int methodResult = await Helpers.IoTHelpers.InvokeDirectMethod(backfillRequest, logger);

                        // if Direct Method failed, enqueue the message
                        if (methodResult != 200)
                        {
                            logger.LogInformation($"{context.FunctionName}: Direct Method Call failed. Enqueueing backfill request message for retry..");
                            try{
                                await outputQueue.SendMessageAsync(
                                    JsonConvert.SerializeObject(backfillRequest),
                                    timeToLive: TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DataGapsTTLSeconds"))),
                                    visibilityTimeout: TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("BackfillRetryVisibilitySeconds"))));
                                logger.LogInformation($"backfill request message is enqueued: {JsonConvert.SerializeObject(backfillRequest)}");
                            }
                            catch (Exception e)
                            {
                                logger.LogError($"{context.FunctionName}: Error enqueueing backfill request message: {e.Message}");
                            }
                        }
                        
                        await Task.Yield();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                ExceptionDispatchInfo.Capture(exceptions.Single()).Throw();
        }
    }

}
