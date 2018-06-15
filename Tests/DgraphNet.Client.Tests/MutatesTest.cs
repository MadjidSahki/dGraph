using DgraphNet.Client.Proto;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DgraphNet.Client.Tests
{
    [TestFixture]
    public class MutatesTest : DgraphIntegrationTest
    {
        private string[] _data = new String[] { "200", "300", "400" };
        private Dictionary<string, string> _uidsMap;

        [Test]
        public async Task test_insert_3_quads()
        {
            var op = new Operation
            {
                Schema = "name: string @index(fulltext) ."
            };

            await _client.AlterAsync(op);

            var txn = _client.NewTransaction();
            _uidsMap = new Dictionary<string, string>();

            foreach (var d in _data)
            {
                var quad = new NQuad
                {
                    Subject = $"_:{d}",
                    Predicate = "name",
                    ObjectValue = new Value { StrVal = $"ok {d}" }
                };

                var mut = new Mutation();
                mut.Set.Add(quad);

                var ag = await txn.MutateAsync(mut);
                _uidsMap.Add(d, ag.Uids[d]);
            }

            await txn.CommitAsync();
            Console.WriteLine("Commit Ok");
        }

        [Test]
        public async Task test_query_3_quads()
        {
            if (_uidsMap == null) await test_insert_3_quads();

            var txn = _client.NewTransaction();
            var uids = _data.Select(x => _uidsMap[x]);

            var query = new StringBuilder()
                .AppendLine("{")
                    .AppendLine($"me(func: uid({string.Join(",", uids)}))")
                    .AppendLine("{")
                        .AppendLine("name")
                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            Console.WriteLine($"Query: {query}");

            var response = await txn.QueryAsync(query);
            var res = response.Json.ToStringUtf8();
            Console.WriteLine($"Responsive JSON: {res}");

            var exp = "{\"me\":[{\"name\":\"ok 200\"},{\"name\":\"ok 300\"},{\"name\":\"ok 400\"}]}";

            Assert.AreEqual(res, exp);
            Assert.IsTrue(response.Txn.StartTs > 0);

            await txn.CommitAsync();
        }
    }
}
