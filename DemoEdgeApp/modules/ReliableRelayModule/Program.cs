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
    using Newtonsoft.Json.Linq;
    using System.Collections.Generic;
    using Microsoft.Extensions.Configuration;
    using ModuleWrapper;
    using AdysTech.InfluxDB.Client.Net;
    using System.IO;
    using ReliableRelayModule.Abstraction;
    using ReliableRelayModule.Service;
    using Serilog;
    using System.Globalization;
    using System.Linq;

    internal class BackfillRequest  
    {
        public string DeviceID { get; set; }
        public string BackfillStartTime { get; set; }
        public string BackfillEndTime { get; set; }
        
        public DateTime CreatedAt { get; set; }
    }
    class TelemetryBatch
    {
        public Guid BatchId { get; set; }
        public List<String> TelemetryDataPoints { get; set; }
    }
    class MethodResponsePayload
    {
        public string DirectMethodResponse { get; set; } = null;
    }

    class Program
    {   
        static int counter;
        static int _skipNextMessage = 0;
        static List<JObject> batch = new List<JObject>();
        static DateTime batchSizeStart;
        static DateTime batchSizeEnd;
        static string influxDBName = Configuration.GetValue("INFLUX_DB_NAME", "opcuatelemetry");
        static string influxDBMeasurementName = Configuration.GetValue("INFLUX_DB_MEASUREMENT_NAME", "telemetry");

        //parameters for code sample telemetry only
        static int batchSizeSecond = 2; 
        static string deviceID = "opcua1"; 

        static public IConfiguration Configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .Build();
        static public IModuleClient ModuleClientDB { get; }
        public IHttpHandler HttpClient { get; }
        static public CancellationTokenSource CancellationTokenSource { get; }
        static public IInfluxDBClient InfluxDBClient = new InfluxDBClient(
                                Configuration.GetValue("INFLUX_URL", "http://influxdb:8086"),
                                Configuration.GetValue("INFLUX_USERNAME", ""),
                                Configuration.GetValue("INFLUX_PASSWORD", ""));
        static public ITimeSeriesRecorder TimeSeriesRecorder = new InfluxDBRecorder(Configuration, ModuleClientDB, CancellationTokenSource,InfluxDBClient);
        
        static Int64 _batchStartTime = (Int64)((long) DateTime.Now.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);

        static void Main(string[] args)
        {
            Init().Wait();
            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };
           
            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);

            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine($"IoT Hub module client initialized.{DateTime.UtcNow}");

            Log.Information("Initializing InfluxDBRecorder");
            await TimeSeriesRecorder.InitializeAsync();
            Console.WriteLine($"InfluxDB initialized.{DateTime.UtcNow}");

            //start batch size window
            batchSizeStart = DateTime.UtcNow;
            batchSizeEnd = batchSizeStart.AddSeconds(batchSizeSecond);

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);

            // Register direct method handlers
            await ioTHubModuleClient.SetMethodHandlerAsync("BackfillMethod", BackfillMethodHandler, ioTHubModuleClient);
            await ioTHubModuleClient.SetMethodHandlerAsync("SkipMessageMethod", SkipMessageMethodHandler, ioTHubModuleClient);
            await ioTHubModuleClient.SetMethodDefaultHandlerAsync(DefaultMethodHandler, ioTHubModuleClient);
        }
        private static Task<MethodResponse> DefaultMethodHandler(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"Received method invocation for non-existing method {methodRequest.Name}. Returning 404.");
            var result = new MethodResponsePayload() { DirectMethodResponse = $"Method {methodRequest.Name} not implemented" };
            var outResult = JsonConvert.SerializeObject(result, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            return Task.FromResult(new MethodResponse(Encoding.UTF8.GetBytes(outResult), 404));
        }
        private static Task<MethodResponse> SkipMessageMethodHandler(MethodRequest methodRequest, object userContext)//SkipMessageMethod direct method
        {
            var result = new MethodResponsePayload() { DirectMethodResponse = $"Next message will be skipped." };
            Interlocked.Exchange(ref _skipNextMessage, 1); // _skipNextMessage flag is set to 1.
            var outResult = JsonConvert.SerializeObject(result, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            return Task.FromResult(new MethodResponse(Encoding.UTF8.GetBytes(outResult), 200));
        }
        private static async Task<MethodResponse> BackfillMethodHandler(MethodRequest methodRequest, object userContext)//BackfillMethod direct method
        {
            var moduleClient = userContext as ModuleClient;
            var request = JsonConvert.DeserializeObject<BackfillRequest>(methodRequest.DataAsJson);
            
            if (string.IsNullOrEmpty(request.DeviceID))
            {
                Console.WriteLine("Backfill received without the Device ID property");

                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "Backfill received without the Device ID property"
                    })), (int)HttpStatusCode.BadRequest);
            }
            
            if (string.IsNullOrEmpty(request.BackfillStartTime) || string.IsNullOrEmpty(request.BackfillEndTime))
            {
                Console.WriteLine("Backfill received without the BackfillStartTime or BackfillEndTime property");

                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "Backfill received without the BackfillStartTime or BackfillEndTime property"
                    })), (int)HttpStatusCode.BadRequest);
            }
            
            //convert Backfill query time window int64 string to DateTime format string, for InfluxDB query
            string BackfillStartTimeDTStr = ConvertLongToUtcTimestamp(long.Parse(request.BackfillStartTime));
            string BackfillEndTimeDTStr = ConvertLongToUtcTimestamp(long.Parse(request.BackfillEndTime));
            String measurementQuery = $"SELECT * FROM {influxDBMeasurementName} WHERE time >= '{BackfillStartTimeDTStr}' AND time <= '{BackfillEndTimeDTStr}'";
            var r = await InfluxDBClient.QueryMultiSeriesAsync (influxDBName, measurementQuery);

            if (r.Count() == 0)
            {
                Console.WriteLine("No data queried within the given query window.");
                return new MethodResponse(
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                    {
                        DirectMethodResponse = "No data queried within the given query window."
                    })), (int)HttpStatusCode.BadRequest);
            }
            foreach (var series in r)
            {
                if (series.HasEntries != true)
                {
                    continue;
                }
                else
                {
                    Console.WriteLine($"Number of queried entries  '{series.SeriesName}': {series.Entries.Count}");
                    try
                    {
                        foreach (var entry in series.Entries)
                        {
                            string entryStr = Newtonsoft.Json.JsonConvert.SerializeObject(entry);
                            string backfillStr = PreprocessBackfillMsg(entryStr);
                            var backfillMessage = new Message(Encoding.UTF8.GetBytes(backfillStr));
                            await moduleClient.SendEventAsync("output1", backfillMessage);
                            Console.WriteLine($"Backfill Message {backfillStr} sent successfully to Edge Hub");  
                        }
                        return new MethodResponse(
                                Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                                {
                                    DirectMethodResponse = "All Backfill Message sent successfully to Edge Hub"
                                })), (int)HttpStatusCode.OK);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Error during Backfill message sending to Edge Hub: {e}");
                        return new MethodResponse(
                            Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                            {
                                DirectMethodResponse = "Backfill Message not sent to Edge Hub"
                            })), (int)HttpStatusCode.InternalServerError);
                    }
                }
            }
            
            return new MethodResponse(
                        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new MethodResponsePayload()
                        {
                            DirectMethodResponse = "Backfill is completed. No Backfill Message queried."
                        })), (int)HttpStatusCode.OK);

        }
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
            
            if (!string.IsNullOrEmpty(messageString)) 
            {
                //flatten the opc ua message and store to influxDB
                JArray jsonArray = JArray.Parse(messageString);
                for (int i = 0; i < jsonArray.Count; i++)
                {
                    JObject flattenItem = new JObject();
                    long timestampUnix = ConvertUtcTimestampToLong(jsonArray[i]["Value"]["SourceTimestamp"]);

                    flattenItem.Add("timestamp", ((Int64)timestampUnix).ToString());
                    flattenItem.Add("deviceID",deviceID);
                    flattenItem.Add("nodeID",jsonArray[i]["NodeId"].ToString());
                    flattenItem.Add("value",jsonArray[i]["Value"]["Value"].ToString());

                    string flattenItemStr = flattenItem.ToString();
                    try
                    {
                        await TimeSeriesRecorder.RecordMessageAsync(flattenItemStr);
                    }
                    catch (Exception ex)
                    {
                        // Ensure the msg is saved successfully into DB before sending to edge hub
                        // If not, skip the msg sending to edge hub.
                        Log.Error(ex, $"Error for storing message {flattenItemStr} into DB");
                        continue;
                    }

                    //Batch the telemetry messages and send the data batch to edge hub
                    batch.Add(flattenItem);
                    if (DateTime.UtcNow > batchSizeEnd)
                    {
                        if (0 == Interlocked.Exchange(ref _skipNextMessage, 0))
                        {
                            //reset batch size window
                            batchSizeStart = DateTime.UtcNow;
                            batchSizeEnd = batchSizeStart.AddSeconds(batchSizeSecond);
                            
                            // Send the batch
                            var batchMessage = CreateBatchMessage(batch);
                            batch.Clear();
                            await moduleClient.SendEventAsync("output1", batchMessage);
                            Console.WriteLine($"Data batch sent to edgehub successfully {DateTime.UtcNow}");    
                        }
                        else{
                            //Simulate a data batch gap by skipping 1 batch and reset the flag to 0.
                            //reset batch size start time window
                            _batchStartTime = (Int64)((long) DateTime.Now.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);

                            //clear batch size
                            batchSizeStart = DateTime.UtcNow;
                            batchSizeEnd = batchSizeStart.AddSeconds(batchSizeSecond);

                            string jsonData = JsonConvert.SerializeObject(batch);
                            batch.Clear();
                            Console.WriteLine($"Data batch skipped to the cloud: {jsonData}");
                        }      
                    }
                }
            }
            return MessageResponse.Completed;
        }
        public static string PreprocessBackfillMsg(string json)
        {
            JObject jsonObj = JObject.Parse(json);

            // update the query result json to the format of the OPC UA message
            jsonObj.Remove("MachineId");
            jsonObj = UpdatePropName(jsonObj, "Time", "timestamp");
            jsonObj = UpdatePropName(jsonObj, "DeviceID", "deviceID");
            jsonObj = UpdatePropName(jsonObj, "NodeID", "nodeID");
            jsonObj = UpdatePropName(jsonObj, "Payload", "value");
            long timestampLong = ConvertUtcTimestampToLong(jsonObj.GetValue("timestamp"));
            jsonObj = UpdatePropValue(jsonObj, "timestamp", jsonObj.GetValue("timestamp").ToString(), ((Int64)timestampLong).ToString());

            return jsonObj.ToString();
        }
        public static JObject UpdatePropName(JObject jObject, string oldName, string newName)
        {
            JToken token;
            if (jObject.TryGetValue(oldName, out token))
            {
                jObject.Remove(oldName);
                jObject[newName] = token;
            }
            return jObject;
        }
        public static JObject UpdatePropValue(JObject jObject, string key, string oldValue, string newValue)
        {          
            JToken token;
            if (jObject.TryGetValue(key, out token))
            {
                token.Replace(newValue); 
            }
            return jObject; 
        }
        public static long ConvertUtcTimestampToLong(JToken timestampJT)
        {
            DateTime timestampDT = DateTime.ParseExact(timestampJT.Value<DateTime>().ToString("O"), "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
            return (long) timestampDT.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        }
        public static string ConvertLongToUtcTimestamp(long timestamp)
        {
            DateTime utcDateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(timestamp);
            return utcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'");
        }
        private static Message CreateBatchMessage(List<JObject> messages)
        {
            try{
                string jsonData = JsonConvert.SerializeObject(messages);
                var batchMessage = new Message(Encoding.UTF8.GetBytes(jsonData));
                batchMessage.Properties.Add("deviceID", messages[messages.Count - 1]["deviceID"].ToString());
                batchMessage.Properties.Add("batchId", Guid.NewGuid().ToString());
                batchMessage.Properties.Add("firstTsInBatch", messages[0]["timestamp"].ToString());
                batchMessage.Properties.Add("lastTsInBatch", messages[messages.Count - 1]["timestamp"].ToString());
                batchMessage.Properties.Add("StartWindow", _batchStartTime.ToString());
                _batchStartTime = (Int64)((long) DateTime.Now.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
                batchMessage.Properties.Add("EndWindow", _batchStartTime.ToString());
                return batchMessage;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in CreateBatchMessage: {e.Message}");
                return null;
            }
        }
    }
}
