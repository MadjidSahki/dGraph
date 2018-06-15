using DgraphNet.Client.Proto;
using Google.Protobuf;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DgraphNet.Client.Tests
{
    [TestFixture]
    public class DeleteEdgeTest : DgraphIntegrationTest
    {
        [Test]
        public async Task test_delete_edges()
        {
            Person alice = new Person
            {
                Name = "Alice",
                Age = 26,
                Married = true,
                Location = "Riley Street"
            };

            School school = new School
            {
                Name = "Crown Public School"
            };

            alice.Schools.Add(school);

            Person bob = new Person
            {
                Name = "Bob",
                Age = 24
            };

            alice.Friends.Add(bob);

            Person charlie = new Person
            {
                Name = "Charlie",
                Age = 29
            };

            alice.Friends.Add(charlie);

            var op = new Operation
            {
                Schema = "age: int .\nmarried: bool ."
            };
            await _client.AlterAsync(op);

            var mut = new Mutation
            {
                CommitNow = true,
                SetJson = ByteString.CopyFromUtf8(JsonConvert.SerializeObject(alice))
            };

            var ag = await _client.NewTransaction().MutateAsync(mut);
            var uid = ag.Uids["blank-0"];

            string q = new StringBuilder()
                .AppendLine("{")
                    .AppendLine($"me(func: uid({uid}))")
                    .AppendLine("{")
                        .AppendLine("uid")
                        .AppendLine("name")
                        .AppendLine("age")
                        .AppendLine("loc")
                        .AppendLine("married")

                        .AppendLine("friends")
                        .AppendLine("{")
                            .AppendLine("uid")
                            .AppendLine("name")
                            .AppendLine("age")
                        .AppendLine("}")

                        .AppendLine("schools")
                        .AppendLine("{")
                            .AppendLine("uid")
                            .AppendLine("name@en")
                        .AppendLine("}")

                    .AppendLine("}")
                .AppendLine("}")
                .ToString();

            var resp = await _client.NewTransaction().QueryAsync(q);
            Console.WriteLine(resp.Json.ToStringUtf8());

            mut = DgraphNetClient.CreateDeleteEdgesMutation(
                new Mutation { CommitNow = true },
                uid,
                "friends", "loc"
            );

            await _client.NewTransaction().MutateAsync(mut);

            resp = await _client.NewTransaction().QueryAsync(q);
            Console.WriteLine(resp.Json.ToStringUtf8());

            var root = JsonConvert.DeserializeObject<Root>(resp.Json.ToStringUtf8());
            Assert.IsTrue(root.Me[0].Friends.Count == 0);
        }

        class School
        {
            public string Uid { get; set; }
            public string Name { get; set; }
        }

        class Person
        {
            public Person()
            {
                Friends = new List<Person>();
                Schools = new List<School>();
            }

            public string Uid { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public bool Married { get; set; }
            public string Location { get; set; }
            public List<Person> Friends { get; set; }
            public List<School> Schools { get; set; }
        }

        class Root
        {
            public List<Person> Me { get; set; }
        }
    }
}
