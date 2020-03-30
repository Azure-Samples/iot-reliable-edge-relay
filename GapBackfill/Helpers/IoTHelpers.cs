
using System;
using System.Threading.Tasks;

using Microsoft.Azure.Devices;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Azure.Samples.ReliableEdgeRelay.Helpers
{
    /// <summary>
    /// Contains helper functions for various IoT operations
    /// </summary>    
    public static class IoTHelpers
    {
        internal static async Task InvokeDirectMethod(Types.BackfillRequest request, ILogger logger)
        {
            try
            {
                logger.LogInformation("Calling BackfillMethod");

                // This call should be non blocking. The back-fill might take a long time on the device.
                // The device needs to ack back and start the back-fill.
                var methodInvocation = new CloudToDeviceMethod(Environment.GetEnvironmentVariable("BackfillMethodName"))
                {
                    ResponseTimeout = TimeSpan.FromSeconds(Int32.Parse(Environment.GetEnvironmentVariable("DirectMethodResponseTimeoutSeconds")))
                };
                methodInvocation.SetPayloadJson(JsonConvert.SerializeObject(request));

                using (var serviceClient = ServiceClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("IoTHubConnectionString")))
                {
                    // Invoke the direct method asynchronously and get the response from the simulated device.
                    logger.LogInformation("Calling BackfillMethod");
                    var response = await serviceClient.InvokeDeviceMethodAsync(
                        Environment.GetEnvironmentVariable("IoTDeviceId"),
                        Environment.GetEnvironmentVariable("IoTModuleId"),
                        methodInvocation);
                    logger.LogInformation($"Response status: {response.Status}, payload: {response.GetPayloadAsJson()}");

                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Error when calling the backfill direct method: {ex.ToString()}");
            }
            return;
        }
    }
}