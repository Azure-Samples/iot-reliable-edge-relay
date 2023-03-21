namespace ReliableRelayModule
{
    using System;
    using System.Net;
    using System.Runtime.Loader;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json;



    /// <summary>
    /// Payload of back-fill direct method request
    /// </summary>
    internal class BackfillRequest
    {
        public DateTime StartWindow { get; set; }
        public DateTime EndWindow { get; set; }
        public string BatchId { get; set; }
        public DateTime Created { get; set; }
    }
    class MethodResponsePayload
    {
        public string DirectMethodResponse { get; set; } = null;
    }

    class Program
    {
        static int counter;
        static int _skipNextMessage = 0;
        static DateTime _dataStartWindow = DateTime.Now;

        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);

            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);

            // Register direct method handlers
            await ioTHubModuleClient.SetMethodHandlerAsync("BackfillMethod", BackfillMethodHandler, ioTHubModuleClient);
            await ioTHubModuleClient.SetMethodHandlerAsync("SkipMessageMethod", SkipMessageMethodHandler, ioTHubModuleClient);

            await ioTHubModuleClient.SetMethodDefaultHandlerAsync(DefaultMethodHandler, ioTHubModuleClient);
        }

        /// <summary>
        /// Default method handler for any method calls which are not implemented
        /// </summary>
        /// <param name="methodRequest"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        private static Task<MethodResponse> DefaultMethodHandler(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"Received method invocation for non-existing method {methodRequest.Name}. Returning 404.");
            var result = new MethodResponsePayload() { DirectMethodResponse = $"Method {methodRequest.Name} not implemented" };
            var outResult = JsonConvert.SerializeObject(result, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            return Task.FromResult(new MethodResponse(Encoding.UTF8.GetBytes(outResult), 404));
        }

        /// <summary>
        /// SkipMessage method handler for any method calls which are not implemented
        /// </summary>
        /// <param name="methodRequest"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        private static Task<MethodResponse> SkipMessageMethodHandler(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"SkipMessage method invocation ");
            var result = new MethodResponsePayload() { DirectMethodResponse = $"Next message will be skipped." };

            Interlocked.Exchange(ref _skipNextMessage, 1); 

            var outResult = JsonConvert.SerializeObject(result, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            return Task.FromResult(new MethodResponse(Encoding.UTF8.GetBytes(outResult), 200));
        }

        /// <summary>
        /// Handler for Backfill
        /// Creates a new IoT Message based on the input from the method request and sends it to the Edge Hub
        /// </summary>
        /// <param name="methodRequest"></param>
        /// <param name="userContext"></param>
        /// <returns></returns>
        private static async Task<MethodResponse> BackfillMethodHandler(MethodRequest methodRequest, object userContext)
        {
            var moduleClient = userContext as ModuleClient;

            var request = JsonConvert.DeserializeObject<BackfillRequest>(methodRequest.DataAsJson);
            if (string.IsNullOrEmpty(request.BatchId))
            {
                Console.WriteLine("Backfill received without correlationId property");

                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "Backfill received without correlationId property"
                    })), (int)HttpStatusCode.BadRequest);
            }

            Console.WriteLine($"Backfill method invocation received. batchId={request.BatchId}. Start={request.StartWindow}. End={request.EndWindow}");

            var message = new Message(Encoding.UTF8.GetBytes($"{{\"backfilled\": \"{request.BatchId}\"}}"))
            {
                ContentType = "application/json",
                ContentEncoding = "UTF-8"
            };
            message.Properties.Add("batchId", request.BatchId);
            message.Properties.Add("isBackfill", "true");
            message.Properties.Add("dataWindowStart", request.StartWindow.ToUniversalTime().ToString("o"));
            message.Properties.Add("dataWindowEnd", request.EndWindow.ToUniversalTime().ToString("o"));

            try
            {
                await moduleClient.SendEventAsync("output1", message);
                Console.WriteLine("Message sent successfully to Edge Hub");

                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "Message sent successfully to Edge Hub"
                    })), (int)HttpStatusCode.OK);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error during message sending to Edge Hub: {e}");

                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "Message not sent to Edge Hub"
                    })), (int)HttpStatusCode.InternalServerError);
            }
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                var pipeMessage = new Message(messageBytes);
                foreach (var prop in message.Properties)
                {
                    pipeMessage.Properties.Add(prop.Key, prop.Value);
                }

                pipeMessage.Properties.Add("dataWindowStart", _dataStartWindow.ToUniversalTime().ToString("o"));
                _dataStartWindow = DateTime.Now;
                pipeMessage.Properties.Add("dataWindowEnd", _dataStartWindow.ToUniversalTime().ToString("o"));

                if (0 == Interlocked.Exchange(ref _skipNextMessage, 0))
                {
                    await moduleClient.SendEventAsync("output1", pipeMessage);
                    Console.WriteLine("Received message sent");
                }
                else
                    Console.WriteLine("Received message skipped");
            }
            return MessageResponse.Completed;
        }
    }
}
