using DgraphNet.Client.Proto;
using Google.Protobuf;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DgraphNet.Client.Extensions;
using System.Linq;

namespace DgraphNet.Client.Tests.Extensions
{
    [TestFixture]
    public class JsonExtensionsTest : DgraphIntegrationTest
    {
        readonly string[] _firsts = new string[] { "Paul", "Eric", "Jack", "John", "Martin" };
        readonly string[] _lasts = new string[] { "Brown", "Smith", "Robinson", "Waters", "Taylor" };
        readonly int[] _ages = new int[] { 20, 25, 30, 35 };

        [OneTimeSetUp]
        public void Setup()
        {
            // Create schema
            string schema = new StringBuilder()
                .AppendLine("first:  string   @index(term)  .")
                .AppendLine("last:   string   @index(hash)  .")
                .AppendLine("age:    int      @index(int)   .")
                .ToString();

            _client.Alter(new Operation { Schema = schema });

            using (var txn = _client.NewTransaction())
            {
                foreach (var f in _firsts)
                    foreach (var l in _lasts)
                        foreach (var a in _ages)
                        {
                            var nqs = new StringBuilder()
                                .AppendLine($"_:acc    <first>    \"{f}\" .")
                                .AppendLine($"_:acc    <last>     \"{l}\" .")
                                .AppendLine($"_:acc    <age>      \"{a}\"^^<xs:int> .")
                                .ToString();

                            var mut = new Mutation
                            {
                                SetNquads = ByteString.CopyFromUtf8(nqs)
                            };

                            txn.Mutate(mut);
                        }

                txn.Commit();
            }
        }

        [Test]
        public void test_query_with_vars_json()
        {
            var q = new StringBuilder()
                .AppendLine("query accounts($terms: string)")
                .AppendLine("{")
                    .AppendLine($"accounts(func: anyofterms(first, $terms))")
                    .AppendLine("{")
                        .AppendLine("first")
                        .AppendLine("last")
                        .AppendLine("age")
                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            var vars = new Dictionary<string, string>
            {
                { "$terms", string.Join(",", _firsts) }
            };

            var res = _client.NewTransaction().QueryWithVars<AccountQuery>(q, vars);

            Assert.IsTrue(_firsts.All(f => res.Accounts.Any(a => a.First == f)));
        }

        [Test]
        public void test_query_json()
        {
            var q = new StringBuilder()
                .AppendLine("{")
                    .AppendLine($"accounts(func: anyofterms(first, \"{string.Join(",", _firsts)}\"))")
                    .AppendLine("{")
                        .AppendLine("first")
                        .AppendLine("last")
                        .AppendLine("age")
                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            var res = _client.NewTransaction().Query<AccountQuery>(q);

            Assert.IsTrue(_firsts.All(f => res.Accounts.Any(a => a.First == f)));
        }

        class AccountQuery
        {
            public Account[] Accounts { get; set; }
        }

        class Account
        {
            public string First { get; set; }
            public string Last { get; set; }
            public int Age { get; set; }
        }
    }
}
