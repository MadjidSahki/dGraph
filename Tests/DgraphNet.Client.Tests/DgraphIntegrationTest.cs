using DgraphNet.Client.Proto;
using Grpc.Core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DgraphNet.Client.Tests
{
    [TestFixture]
    public class DgraphIntegrationTest
    {
        protected DgraphNetClient _client;

        protected const string HOSTNAME = "localhost";
        protected const string PORT = "9080";

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            var pool = new DgraphConnectionPool()
                .Add(new Channel($"{HOSTNAME}:{PORT}", ChannelCredentials.Insecure));

            _client = new DgraphNetClient(pool);

            await _client.AlterAsync(new Operation { DropAll = true });
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await _client.Pool.CloseAllAsync();
        }
    }
}
