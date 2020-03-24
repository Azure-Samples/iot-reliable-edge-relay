using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

namespace Azure.Samples.ReliableEdgeRelay.Helpers
{
    /// <summary>
    /// Contains helper functions for various SQL operations
    /// </summary>
    public static class SqlHelpers
    {

        internal static string SQL_GET_GAPS = @"
        WITH Ordered AS ( SELECT *, ROW_NUMBER() OVER (ORDER BY startWindow) RN FROM [dbo].[DataWindows] ) 
        SELECT DISTINCT o1.BatchId BatchId, o2.EndWindow StartWindow, o1.StartWindow EndWindow, DATEDIFF(s, CAST(o2.EndWindow AS datetime2), CAST(o1.StartWindow AS datetime2)) GapInSeconds 
        FROM Ordered o1 JOIN Ordered o2 ON o1.RN  = o2.RN + 1 AND o1.BatchId = o2.BatchId 
        WHERE o1.StartWindow <> o2.EndWindow AND o1.BatchId = @batchId AND CAST(o1.StartWindow AS datetime2) >= DATEADD(second,-@timeout, GETDATE())
        ORDER BY StartWindow DESC;";


        internal static string SQL_GET_GAPS_IN_WINDOW = @"
        WITH Ordered AS ( SELECT StartWindow, EndWindow, BatchId, ROW_NUMBER() OVER (ORDER BY StartWindow) RN FROM [dbo].[DataWindows] )
        SELECT DISTINCT o1.BatchId BatchId, o2.EndWindow StartWindow, o1.StartWindow EndWindow, DATEDIFF(s, CAST(o2.EndWindow AS datetime2),CAST(o1.StartWindow AS datetime2)) GapInSeconds 
        FROM Ordered o1 JOIN Ordered o2 ON o1.RN = o2.RN + 1 AND o1.BatchId = o2.BatchId
        WHERE o1.StartWindow <> o2.EndWindow and o1.BatchId = @batchId AND o1.StartWindow >= @startWindow
        AND o2.EndWindow <= @startWindow AND o1.StartWindow >= @endWindow
        ORDER BY StartWindow DESC;";

        internal static string SQL_INSERT_BACKFILL_REQUEST = @"
        INSERT INTO [dbo].[BackfillDeviceRequests]
        VALUES (@startWindow, @endWindow, @batchId, @Created);";


        internal static string SQL_GET_EXISTING_BACKFILL_REQUEST = @"
        SELECT TOP (1) * FROM [dbo].[BackfillDeviceRequests]
        WHERE StartWindow >= @startWindow AND EndWindow <= @endWindow AND BatchId = @batchId AND CAST(Created AS datetime2) > DATEADD(second,-@timeout,GETDATE())
        ORDER BY Created DESC";

        public static async Task<List<Types.DataGap>> GetDataGaps(Types.OrphanMessage orphanMessage, ILogger logger)
        {
            var dataGaps = new List<Types.DataGap>();
            try
            {
                using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("SQLConnectionString")))
                {
                    await connection.OpenAsync();

                    using (SqlCommand command = new SqlCommand(SqlHelpers.SQL_GET_GAPS, connection))
                    {
                        command.Parameters.AddWithValue("@batchId", orphanMessage.BatchId);
                        command.Parameters.AddWithValue("@timeout", Int32.Parse(Environment.GetEnvironmentVariable("DataGapsTTLSeconds")));
                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                dataGaps.Add(new Types.DataGap()
                                {
                                    BatchId = reader.GetString(0),
                                    StartWindow = DateTime.Parse(reader.GetString(1)).ToUniversalTime(),
                                    EndWindow = DateTime.Parse(reader.GetString(2)).ToUniversalTime(),
                                    GapInSeconds = reader.GetInt32(3)
                                });
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                logger.LogError($"Error when reading sql database: {e.ToString()}");
                throw;
            }
            return dataGaps;
        }


        internal static async Task<List<Types.DataGap>> CalculateActualGaps(Types.DataGap dataGap, ILogger logger)
        {
            var dataGaps = new List<Types.DataGap>();
            try
            {
                using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("SQLConnectionString")))
                {
                    await connection.OpenAsync();

                    using (SqlCommand command = new SqlCommand(SqlHelpers.SQL_GET_GAPS_IN_WINDOW, connection))
                    {
                        command.Parameters.AddWithValue("@batchId", dataGap.BatchId);
                        command.Parameters.AddWithValue("@startWindow", dataGap.StartWindow.ToUniversalTime().ToString("o"));
                        command.Parameters.AddWithValue("@endWindow", dataGap.EndWindow.ToUniversalTime().ToString("o"));

                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                dataGaps.Add(new Types.DataGap()
                                {
                                    BatchId = reader.GetString(0),
                                    StartWindow = DateTime.Parse(reader.GetString(1)).ToUniversalTime(),
                                    EndWindow = DateTime.Parse(reader.GetString(2)).ToUniversalTime(),
                                    GapInSeconds = reader.GetInt32(3)
                                });
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                logger.LogError($"Error when reading sql database: {e.ToString()}");
                throw;
            }
            return dataGaps;
        }

        internal static Types.BackfillRequest CreateDeviceBackfillRequest(Types.DataGap dataGap, ILogger logger)
        {
            Types.BackfillRequest backfillRequest = new Types.BackfillRequest()
            {
                BatchId = dataGap.BatchId,
                EndWindow = dataGap.EndWindow,
                StartWindow = dataGap.StartWindow,
                Created = DateTime.UtcNow
            };
            return backfillRequest;
        }

        internal static async Task RecordDeviceBackfillRequest(Types.BackfillRequest request, ILogger logger)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("SQLConnectionString")))
                {
                    await connection.OpenAsync();

                    using (SqlCommand command = new SqlCommand(SqlHelpers.SQL_INSERT_BACKFILL_REQUEST, connection))
                    {
                        command.Parameters.AddWithValue("@startWindow", request.StartWindow.ToUniversalTime().ToString("o"));
                        command.Parameters.AddWithValue("@endWindow", request.EndWindow.ToUniversalTime().ToString("o"));
                        command.Parameters.AddWithValue("@batchId", request.BatchId);
                        command.Parameters.AddWithValue("@created", request.Created.ToUniversalTime().ToString("o"));

                        if (await command.ExecuteNonQueryAsync() != 1)
                            throw new Exception($"Cannot insert new backfill request in SQL:{JsonConvert.SerializeObject(request)}");
                    }
                }
            }
            catch (SqlException e)
            {
                logger.LogError($"Error when reading sql database: {e.ToString()}");
                throw;
            }
            return;
        }

        internal static async Task<Types.BackfillRequest> GetExistingDeviceBackfillRequest(Types.DataGap request, ILogger logger)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("SQLConnectionString")))
                {
                    await connection.OpenAsync();

                    using (SqlCommand command = new SqlCommand(SqlHelpers.SQL_GET_EXISTING_BACKFILL_REQUEST, connection))
                    {
                        command.Parameters.AddWithValue("@startWindow", request.StartWindow.ToUniversalTime().ToString("o"));
                        command.Parameters.AddWithValue("@endWindow", request.StartWindow.ToUniversalTime().ToString("o"));
                        command.Parameters.AddWithValue("@batchId", request.BatchId);
                        command.Parameters.AddWithValue("@timeout", Int32.Parse(Environment.GetEnvironmentVariable("BackfillTimeout")));

                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
                        {
                            if (await reader.ReadAsync())
                            {
                                return new Types.BackfillRequest()
                                {
                                    StartWindow = DateTime.Parse(reader.GetString(0)).ToUniversalTime(),
                                    EndWindow = DateTime.Parse(reader.GetString(1)).ToUniversalTime(),
                                    BatchId = reader.GetString(2),
                                    Created = DateTime.Parse(reader.GetString(3)).ToUniversalTime()
                                };
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                logger.LogError($"Error when reading sql database: {e.ToString()}");
                throw;
            }
            return null;
        }
    }
}