using DgraphNet.Client.Proto;
using Google.Protobuf;
using Grpc.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static DgraphNet.Client.Proto.Dgraph;

namespace DgraphNet.Client.Tests
{
    [TestFixture]
    public class DgraphNetTest : DgraphIntegrationTest
    {
        [SetUp]
        public async Task BeforeEach()
        {
            await _client.AlterAsync(new Operation { DropAll = true });
        }

        [Test]
        public void test_merge_context()
        {
            var dst = new LinRead();
            dst.Ids.Add(new Dictionary<uint, ulong>
            {
                { 1, 10L },
                { 2, 15L },
                { 3, 10L }
            });

            var src = new LinRead();
            src.Ids.Add(new Dictionary<uint, ulong>
            {
                { 2, 10L },
                { 3, 15L },
                { 4, 10L }
            });

            var result = DgraphNetClient.MergeLinReads(dst, src);

            Assert.AreEqual(4, result.Ids.Count);

            Assert.AreEqual(10L, result.Ids[1]);
            Assert.AreEqual(15L, result.Ids[2]);
            Assert.AreEqual(15L, result.Ids[3]);
            Assert.AreEqual(10L, result.Ids[4]);
        }

        [Test]
        public async Task test_txn_query_variables()
        {
            // Set schema
            var op = new Operation { Schema = "name: string @index(exact) ." };
            await _client.AlterAsync(op);

            // Add data
            var json = new JObject();
            json.Add("name", "Alice");

            var mut = new Mutation
            {
                CommitNow = true,
                SetJson = ByteString.CopyFromUtf8(json.ToString())
            };

            await _client.NewTransaction().MutateAsync(mut);

            // Query
            string query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
            var vars = new Dictionary<string, string>
            {
                { "$a", "Alice" }
            };

            var res = await _client.NewTransaction().QueryWithVarsAsync(query, vars);

            // Verify data as expected
            json = JObject.Parse(res.Json.ToStringUtf8());
            Assert.IsTrue(json.ContainsKey("me"));

            var arr = json.GetValue("me") as JArray;
            var obj = arr[0] as JObject;
            var name = obj.Property("name").Value.ToString();

            Assert.AreEqual("Alice", name);
        }

        [Test]
        public async Task test_delete()
        {
            using (var txn = _client.NewTransaction())
            {
                var mutation = new Mutation
                {
                    SetNquads = ByteString.CopyFromUtf8("<_:bob> <name> \"Bob\" .")
                };

                var ag = await txn.MutateAsync(mutation);
                string bob = ag.Uids["bob"];

                string query = new StringBuilder()
                    .AppendLine("{")
                        .AppendLine($"find_bob(func: uid({bob}))")
                        .AppendLine("{")
                            .AppendLine("name")
                        .AppendLine("}")
                    .AppendLine("}")
                    .ToString();

                var resp = await txn.QueryAsync(query);
                var json = JObject.Parse(resp.Json.ToStringUtf8());

                var arr = json.GetValue("find_bob") as JArray;
                Assert.IsTrue(arr.Count > 0);

                mutation = new Mutation
                {
                    DelNquads = ByteString.CopyFromUtf8($"<{bob}> * * .")
                };

                await txn.MutateAsync(mutation);

                resp = await txn.QueryAsync(query);
                json = JObject.Parse(resp.Json.ToStringUtf8());

                arr = json.GetValue("find_bob") as JArray;
                Assert.IsTrue(arr.Count == 0);

                await txn.CommitAsync();
            }
        }

        [Test]
        public void test_commit_after_CommitNow()
        {
            Assert.ThrowsAsync<TxnFinishedException>(async () =>
            {
                using (var txn = _client.NewTransaction())
                {
                    var mut = new Mutation
                    {
                        SetNquads = ByteString.CopyFromUtf8("<_:bob> <name> \"Bob\" ."),
                        CommitNow = true
                    };

                    await txn.MutateAsync(mut);
                    await txn.CommitAsync();
                }
            });
        }

        [Test]
        public async Task test_discard_abort()
        {
            using (var txn = _client.NewTransaction())
            {
                var mut = new Mutation
                {
                    SetNquads = ByteString.CopyFromUtf8("<_:bob> <name> \"Bob\" ."),
                    CommitNow = true
                };

                await txn.MutateAsync(mut);
            }
        }

        [Test]
        public async Task test_client_with_deadline()
        {
            var channel = new Channel($"{HOSTNAME}:{PORT}", ChannelCredentials.Insecure);
            var client = new DgraphNetClient(new DgraphConnection(channel), 1);

            var op = new Operation { Schema = "name: string @index(exact) ." };

            // Alters schema without exceeding the given deadline.
            await client.AlterAsync(op);

            // Creates a blocking stub directly, in order to force a deadline to be exceeded.
            var anyConnectionMethod = typeof(DgraphNetClient)
                .GetMethod("AnyConnection", BindingFlags.NonPublic | BindingFlags.Instance);

            var (conn, callOptions) = (ValueTuple<DgraphConnection, CallOptions>)anyConnectionMethod
                .Invoke(client, Array.Empty<object>());

            var stub = conn.Client;

            Thread.Sleep(1001);

            Assert.Throws<RpcException>(() => stub.Alter(op, callOptions), "Deadline should have been exceeded");
        }
    }
}
