using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Azure.Samples.ReliableEdgeRelay.Functions
{
    /// <summary>
    /// An Azure Function that receives back-fill enqueued requests, checks for existing overalapping requests
    /// and creates the output events for actual back-fill execution
    /// </summary>    
    public static class BackfillRequestProcessor
    {
        [FunctionName("ProcessBackfillRequest")]
        public static async Task RunAsync(
            [QueueTrigger("%StorageQueueName%", Connection = "StorageConnectionString")] string backfillRequest,
            [EventHub("%ExecutionEventHubName%", Connection = "EventHubConnectionString")] IAsyncCollector<string> outputEvents,
            ILogger log,
            ExecutionContext context)
        {
            try
            {
                log.LogInformation($"{context.FunctionName}: {backfillRequest}");

                Types.DataGap request = JsonConvert.DeserializeObject<Types.DataGap>(backfillRequest);

                Types.BackfillRequest existingDeviceBackfillRequest = await Helpers.SqlHelpers.GetExistingDeviceBackfillRequest(request, log);

                if (existingDeviceBackfillRequest != null)
                {
                    log.LogInformation($"{context.FunctionName}: Found existing backfill request {JsonConvert.SerializeObject(existingDeviceBackfillRequest)}");
                    return;
                }

                List<Types.DataGap> latestGaps = await Helpers.SqlHelpers.CalculateActualGaps(request, log);
                foreach (Types.DataGap gap in latestGaps)
                {
                    existingDeviceBackfillRequest = await Helpers.SqlHelpers.GetExistingDeviceBackfillRequest(request, log);
                    if (existingDeviceBackfillRequest != null)
                    {
                        log.LogInformation($"{context.FunctionName}: Found existing backfill request {JsonConvert.SerializeObject(existingDeviceBackfillRequest)}");
                        continue;
                    }

                    Types.BackfillRequest requestSection = Helpers.SqlHelpers.CreateDeviceBackfillRequest(gap, log);

                    string requestSectionString = JsonConvert.SerializeObject(requestSection);

                    await outputEvents.AddAsync(requestSectionString);

                    log.LogInformation($"{context.FunctionName}: Created backfill request {requestSectionString}");
                }

            }
            catch (Exception e)
            {
                log.LogError($"Error in {context.FunctionName}: {e}");
                throw;
            }
        }
    }
}
