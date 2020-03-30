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
    /// An Azure Function that receives a back-fill request. This function will execute the back-fill
    /// request via an Edge direct method call and enqueue a follow-up backfill request message.
    /// This message will become visible after the back-fill request expires and it's here to ensure
    /// that the device completed the back-fill request
    /// </summary>    
    public static class BackfillRequestExecutor
    {
        [FunctionName("ExecuteBackfillRequest")]
        public static async Task RunAsync(
            [EventHubTrigger("%ExecutionEventHubName%", Connection = "EventHubConnectionString")] EventData[] inputEvent,
            [Queue("%StorageQueueName%", Connection = "StorageConnectionString")] CloudQueue outputQueue,
            ILogger logger,
            ExecutionContext context)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in inputEvent)
            {
                try
                {
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    logger.LogInformation($"{context.FunctionName}: {messageBody}");

                    // AzFunctions will append multiple messages in the same event
                    var messageBodies = messageBody.Split(System.Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

                    foreach (string message in messageBodies)
                    {
                        var backfillDeviceRequest = JsonConvert.DeserializeObject<Types.BackfillRequest>(message);

                        // We can check for existing overlapping records here to avoid any rare race conditions
                        await Helpers.SqlHelpers.RecordDeviceBackfillRequest(backfillDeviceRequest, logger);

                        await Helpers.IoTHelpers.InvokeDirectMethod(backfillDeviceRequest, logger);

                        await outputQueue.AddMessageAsync(
                                message: new CloudQueueMessage(
                                    JsonConvert.SerializeObject(new Types.DataGap()
                                    {
                                        BatchId = backfillDeviceRequest.BatchId,
                                        StartWindow = backfillDeviceRequest.StartWindow,
                                        EndWindow = backfillDeviceRequest.EndWindow,
                                        GapInSeconds = (backfillDeviceRequest.EndWindow - backfillDeviceRequest.StartWindow).Seconds
                                    })),
                                timeToLive: TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DataGapsTTLSeconds"))),
                                initialVisibilityDelay: TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DetectionRetryInvincibleSeconds"))),
                                options: null,
                                operationContext: null);

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
