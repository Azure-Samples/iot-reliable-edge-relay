using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;

namespace Azure.Samples.ReliableEdgeRelay.Functions
{
    public static class RetryBackfillRequest
    {
        //retry policy with 10s, 60s, 5m intervals
        private static readonly AsyncRetryPolicy RetryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(new[]
        {
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMinutes(5)
        });

        [FunctionName("RetryBackfillRequest")]
        public static async Task RunAsync(
            [QueueTrigger("%StorageQueueName%", Connection = "StorageConnectionString")] string backfillRequest,
            ILogger logger,
            ExecutionContext context)
        {
            var exceptions = new List<Exception>();

            try
            {
                logger.LogInformation($"{context.FunctionName}: {backfillRequest}");

                Types.BackfillRequest retryRequest = JsonConvert.DeserializeObject<Types.BackfillRequest>(backfillRequest);
                int methodResult = await RetryPolicy.ExecuteAsync(async () =>
                {
                    return await Helpers.IoTHelpers.InvokeDirectMethod(retryRequest, logger);
                });

                if (methodResult != 200)
                {
                    logger.LogInformation($"{context.FunctionName}: retry Direct Method Call failed. wait for next retry..");
                }
                else
                {
                    logger.LogInformation($"{context.FunctionName}: retry Direct Method Call succeeded.");
                }

                await Task.Yield();
            }
            catch (Exception e)
            {
                // Catch any exception from the retry policy and add it to the list of exceptions to throw at the end
                exceptions.Add(e);
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                ExceptionDispatchInfo.Capture(exceptions.Single()).Throw();
        }
    }
}
