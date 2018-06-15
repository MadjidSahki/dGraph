using DgraphNet.Client.Proto;
using Google.Protobuf;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DgraphNet.Client.Tests
{
    [TestFixture]
    public class AcctUpsertTest : DgraphIntegrationTest
    {
        int _lastStatus;
        int _successCount;
        int _retryCount;

        readonly string[] _firsts = new string[] { "Paul", "Eric", "Jack", "John", "Martin" };
        readonly string[] _lasts = new string[] { "Brown", "Smith", "Robinson", "Waters", "Taylor" };
        readonly int[] _ages = new int[] { 20, 25, 30, 35 };

        List<Account> _accounts = new List<Account>();

        private async Task Setup()
        {
            foreach (var f in _firsts)
                foreach (var l in _lasts)
                    foreach (var a in _ages)
                    {
                        _accounts.Add(new Account
                        {
                            First = f,
                            Last = l,
                            Age = a
                        });
                    }

            string schema = new StringBuilder()
                .AppendLine("first:  string   @index(term)  @upsert .")
                .AppendLine("last:   string   @index(hash)  @upsert .")
                .AppendLine("age:    int      @index(int)   @upsert .")
                .AppendLine("when:   int      @index(int)   @upsert .")
                .ToString();

            await _client.AlterAsync(new Operation { Schema = schema });
        }

        private async Task TryUpsert(Account account)
        {
            var txn = _client.NewTransaction();

            var query = new StringBuilder()
                .AppendLine("{")
                    .AppendLine($"get(func: eq(first, \"{account.First}\")) @filter(eq(last, \"{account.Last}\") AND eq(age, {account.Age}))")
                    .AppendLine("{")
                        .AppendLine("uid: _uid_")
                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            try
            {
                var resp = await txn.QueryAsync(query);
                var decode1 = JsonConvert.DeserializeObject<Decode1>(resp.Json.ToStringUtf8());

                Assert.IsTrue(decode1.Get.Count <= 1);

                string uid;

                if (decode1.Get.Count == 1)
                {
                    uid = decode1.Get[0].Uid;
                }
                else
                {
                    string nqs = new StringBuilder()
                        .AppendLine($"_:acct <first> \"{account.First}\" .")
                        .AppendLine($"_:acct <last>  \"{account.Last}\" .")
                        .AppendLine($"_:acct <age>   \"{account.Age}\"^^<xs:int> .")
                        .ToString();

                    var mut1 = new Mutation
                    {
                        SetNquads = ByteString.CopyFromUtf8(nqs)
                    };

                    var assigned = await txn.MutateAsync(mut1);

                    uid = assigned.Uids["acct"];
                }

                string nq = $"<{uid}> <when> \"{DateTime.UtcNow.Ticks}\"^^<xs:int> .";

                var mut2 = new Mutation
                {
                    SetNquads = ByteString.CopyFromUtf8(nq)
                };

                await txn.MutateAsync(mut2);
                await txn.CommitAsync();
            }
            finally
            {
                await txn.DiscardAsync();
            }
        }

        private async Task Upsert(Account account)
        {
            while (true)
            {
                var elapsed = DateTime.Now.Millisecond - _lastStatus;

                if (elapsed > 100)
                {
                    Console.WriteLine($"Success: {_successCount}, Retries: {_retryCount}");
                    _lastStatus = DateTime.Now.Millisecond;
                };

                try
                {
                    await TryUpsert(account);
                    Interlocked.Increment(ref _successCount);
                    return;
                }
                catch (Exception e)
                {
                    Interlocked.Increment(ref _retryCount);
                }
            }
        }

        private async Task DoUpserts()
        {
            var tasks = new List<Task>();

            foreach (var a in _accounts)
            {
                tasks.AddRange(Enumerable
                    .Range(0, 5)
                    .Select(_ => Task.Run(() => Upsert(a))));
            }

            await Task.WhenAll(tasks);
        }

        private async Task CheckIntegrity()
        {
            string q = new StringBuilder()
                .AppendLine("{")
                    .AppendLine($"all(func: anyofterms(first, \"{string.Join(",", _firsts)}\"))")
                    .AppendLine("{")
                        .AppendLine("first")
                        .AppendLine("last")
                        .AppendLine("age")
                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            var resp = await _client.NewTransaction().QueryAsync(q);
            var decode2 = JsonConvert.DeserializeObject<Decode2>(resp.Json.ToStringUtf8());

            var accountSet = new HashSet<string>();

            foreach (var record in decode2.All)
            {
                Assert.IsTrue(!string.IsNullOrEmpty(record.First));
                Assert.IsTrue(!string.IsNullOrEmpty(record.Last));
                Assert.IsTrue(record.Age != 0);

                string entry = $"{record.First}_{record.Last}_{record.Age}";

                accountSet.Add(entry);
            }

            Assert.IsTrue(accountSet.Count == _accounts.Count);

            foreach (var a in _accounts)
            {
                string entry = $"{a.First}_{a.Last}_{a.Age}";
                Assert.IsTrue(accountSet.Contains(entry));
            }
        }

        [Test]
        public async Task test_acct_upsert()
        {
            await Setup();
            await DoUpserts();
            await CheckIntegrity();
        }

        class Account
        {
            public string First { get; set; }
            public string Last { get; set; }
            public int Age { get; set; }
        }

        class Decode1
        {
            public List<Uids> Get { get; set; }

            public class Uids
            {
                public string Uid { get; set; }
            }
        }

        class Decode2
        {
            public List<Entry> All { get; set; }

            public class Entry
            {
                public string First { get; set; }
                public string Last { get; set; }
                public int Age { get; set; }
            }
        }
    }
}
