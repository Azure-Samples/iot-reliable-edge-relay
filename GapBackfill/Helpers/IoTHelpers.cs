using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
namespace Azure.Samples.ReliableEdgeRelay.Helpers
{
    public static class IoTHelpers
    {
        public static async Task<int> InvokeDirectMethod(Types.BackfillRequest request, ILogger logger)
        {
            try
            {
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
                    return response.Status;

                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Error when calling the backfill direct method: {ex.ToString()}");
                return (int)HttpStatusCode.InternalServerError;
            }
        }
    }
}
