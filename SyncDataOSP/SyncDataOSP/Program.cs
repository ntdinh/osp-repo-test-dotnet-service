using Confluent.Kafka;
using Elastic.Clients.Elasticsearch;
using MongoDB.Driver;
//using MongoDB.Driver.Core.Configuration;
using Nest;
using Newtonsoft.Json;
//using Npgsql;

namespace SyncDataOSP
{
    internal class Program
    {
    private static readonly string KAFKA_BROKER_URL = Environment.GetEnvironmentVariable("KAFKA_BROKER_URL") ?? "localhost:9092";
    private static readonly List<string> TOPICS = new List<string> { "dbserver1.public.orders" };

    private static readonly string POSTGRES_CONN_STRING = "Host=localhost;Database=dinhnt;Username=postgres;Password=dinhnt";
    private static readonly string MONGO_CONN_STRING = Environment.GetEnvironmentVariable("MONGO_CONN_STRING") ?? "mongodb://localhost:27017/";
    private static readonly string MONGO_DB = "dinhnt";
    private static readonly string MONGO_COLLECTION = "users";
        static void Main(string[] args)
        {
            try
            {
                using var pgConn = new Npgsql.NpgsqlConnection(POSTGRES_CONN_STRING);
                pgConn.Open();

                var mongoClient = new MongoClient(MONGO_CONN_STRING);
                var mongoDb = mongoClient.GetDatabase(MONGO_DB);
                var mongoCollection = mongoDb.GetCollection<Dictionary<string, object>>(MONGO_COLLECTION);

                Console.WriteLine("✅ Connected to PostgreSQL and MongoDB.");

                var config = new ConsumerConfig
                {
                    BootstrapServers = KAFKA_BROKER_URL,
                    GroupId = "mongodb-sync-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true
                };

                using var consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(TOPICS);

                while (true)
                {
                    try
                    {
                        // Get message từ Kafka
                        var cr = consumer.Consume(CancellationToken.None);
                        var messageJson = JsonConvert.DeserializeObject<Dictionary<string, object>>(cr.Message.Value);

                        if (messageJson == null || !messageJson.ContainsKey("payload")) continue;

                        var payload = JsonConvert.DeserializeObject<Dictionary<string, object>>(messageJson["payload"].ToString());
                        if (payload == null) continue;

                        var op = payload.ContainsKey("op") ? payload["op"]?.ToString() : null;

                        // kiểm tra insert/update/delete
                        var data = op != "d"
                            ? JsonConvert.DeserializeObject<Dictionary<string, object>>(payload["after"]?.ToString() ?? "{}")
                            : JsonConvert.DeserializeObject<Dictionary<string, object>>(payload["before"]?.ToString() ?? "{}");

                        if (data == null || !data.ContainsKey("order_id")) continue;
                        var orderId = data["order_id"];
                        var fullDocument = GetFullOrderDetails(pgConn, orderId);
                        // Update To Mongo
                        if (op == "d" && cr.Topic.EndsWith("orders"))
                        {
                            mongoCollection.DeleteOne(Builders<Dictionary<string, object>>.Filter.Eq("_id", orderId));
                        }
                        else
                        {
                            if (fullDocument != null)
                            {
                                mongoCollection.ReplaceOne(
                                    Builders<Dictionary<string, object>>.Filter.Eq("_id", fullDocument["_id"]),
                                    fullDocument,
                                    new ReplaceOptions { IsUpsert = true }
                                );
                                Console.WriteLine($"✅ Synced document for order_id: {orderId} to MongoDB.");
                            }
                        }

                        // Update to Elastic Search

                        SyncDataToElasticSearch(fullDocument);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"🔥 An error occurred while processing message: {ex.StackTrace}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"🔥 Could not connect to databases: {ex.StackTrace}");
            }
            Console.WriteLine("Hello, World!");
        }

        private static Dictionary<string, object> GetFullOrderDetails(Npgsql.NpgsqlConnection conn, object orderId)
        {
            string query = $"select * from public.orders where order_id = '{orderId}'";

            try
            {
                using var cmd = new Npgsql.NpgsqlCommand(query, conn);
                using var reader = cmd.ExecuteReader();

                if (!reader.Read()) return null;

                var result = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    result[reader.GetName(i)] = reader.GetValue(i);
                }

                result["_id"] = result["order_id"];
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching data for order_id {orderId}: {ex.StackTrace}");
                return null;
            }
        }

        public static void SyncDataToElasticSearch(Dictionary<string, object> data)
        {
            var url = new Uri("https://elasticsearch.ptgppplus.org:9200");
            var index = "test";

            var settings = new ConnectionSettings(url)
                .DefaultIndex(index) // Thay tên index theo yêu cầu
                .BasicAuthentication("elastic", "Wr6f66yDgTwJ7872SW6LN8W6") // user & password

                // Nếu server Elastic dùng TLS không chuẩn (self-signed)
                .ServerCertificateValidationCallback((o, certificate, chain, errors) => true);
            var client = new ElasticClient(settings);
            if (!data.TryGetValue("_id", out var id))
            {
                Console.WriteLine("Không tìm thấy khóa '_id' trong dữ liệu.");
                return;
            }

            var descriptor = new BulkDescriptor();
            descriptor.Update<Dictionary<string, object>>(d => d.Id(id.ToString()).Index(index).Doc(data).DocAsUpsert(true)).Refresh((Elasticsearch.Net.Refresh?)Refresh.WaitFor);
            client.Bulk(descriptor);
        }
        //public static void SyncDataToMongo()
        //{
        //    try
        //    {
        //        using var pgConn = new NpgsqlConnection(POSTGRES_CONN_STRING);
        //        pgConn.Open();

        //        var mongoClient = new MongoClient(MONGO_CONN_STRING);
        //        var mongoDb = mongoClient.GetDatabase(MONGO_DB);
        //        var mongoCollection = mongoDb.GetCollection<Dictionary<string, object>>(MONGO_COLLECTION);

        //        Console.WriteLine("✅ Connected to PostgreSQL and MongoDB.");

        //        var config = new ConsumerConfig
        //        {
        //            BootstrapServers = KAFKA_BROKER_URL,
        //            GroupId = "mongodb-sync-group",
        //            AutoOffsetReset = AutoOffsetReset.Earliest,
        //            EnableAutoCommit = true
        //        };

        //        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        //        consumer.Subscribe(TOPICS);

        //        while (true)
        //        {
        //            try
        //            {
        //                // Get message từ Kafka
        //                var cr = consumer.Consume(CancellationToken.None);
        //                var messageJson = JsonConvert.DeserializeObject<Dictionary<string, object>>(cr.Message.Value);

        //                if (messageJson == null || !messageJson.ContainsKey("payload")) continue;

        //                var payload = JsonConvert.DeserializeObject<Dictionary<string, object>>(messageJson["payload"].ToString());
        //                if (payload == null) continue;

        //                var op = payload.ContainsKey("op") ? payload["op"]?.ToString() : null;

        //                // kiểm tra insert/update/delete
        //                var data = op != "d"
        //                    ? JsonConvert.DeserializeObject<Dictionary<string, object>>(payload["after"]?.ToString() ?? "{}")
        //                    : JsonConvert.DeserializeObject<Dictionary<string, object>>(payload["before"]?.ToString() ?? "{}");

        //                if (data == null || !data.ContainsKey("order_id")) continue;
        //                var orderId = data["order_id"];

        //                if (op == "d" && cr.Topic.EndsWith("orders"))
        //                {
        //                    mongoCollection.DeleteOne(Builders<Dictionary<string, object>>.Filter.Eq("_id", orderId));
        //                }
        //                else
        //                {
        //                    var fullDocument = GetFullOrderDetails(pgConn, orderId);
        //                    if (fullDocument != null)
        //                    {
        //                        mongoCollection.ReplaceOne(
        //                            Builders<Dictionary<string, object>>.Filter.Eq("_id", fullDocument["_id"]),
        //                            fullDocument,
        //                            new ReplaceOptions { IsUpsert = true }
        //                        );
        //                        Console.WriteLine($"✅ Synced document for order_id: {orderId} to MongoDB.");
        //                    }
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                Console.WriteLine($"🔥 An error occurred while processing message: {ex.StackTrace}");
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"🔥 Could not connect to databases: {ex.StackTrace}");
        //    }
        //}
    }
}
