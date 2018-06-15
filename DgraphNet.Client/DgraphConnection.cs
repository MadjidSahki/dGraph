using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using static DgraphNet.Client.Proto.Dgraph;

namespace DgraphNet.Client
{
    /// <summary>
    /// A gRPC connection to a Dgraph server.
    /// </summary>
    public class DgraphConnection
    {
        Channel _channel;
        DgraphClient _client;

        /// <summary>
        /// Creates a new gRPC connection to a Dgraph server with the specified channel.
        /// </summary>
        /// <param name="channel">A configured gRPC channel.</param>
        public DgraphConnection(Channel channel)
        {
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
            _client = new DgraphClient(channel);
        }

        /// <summary>
        /// Creates a new gRPC connection to a Dgraph server with the specified channel.
        /// </summary>
        /// <param name="host">Host of the Dgraph server.</param>
        /// <param name="port">Port of the Dgraph server.</param>
        /// <param name="credentials">Credentials for the connection.</param>
        public DgraphConnection(string host, int port, ChannelCredentials credentials)
            : this(new Channel(host, port, credentials))
        {

        }

        /// <summary>
        /// Creates a new gRPC connection to a Dgraph server with the specified channel. 
        /// <para/><see cref="ChannelCredentials.Insecure"/> will be used for the connection.
        /// </summary>
        /// <param name="host">Host of the Dgraph server.</param>
        /// <param name="port">Port of the Dgraph server.</param>
        public DgraphConnection(string host, int port)
            : this(host, port, ChannelCredentials.Insecure)
        {
        }

        /// <summary>
        /// The channel used for the connection.
        /// </summary>
        public Channel Channel => _channel;

        /// <summary>
        /// The current state of the connection.
        /// </summary>
        public ChannelState State => _channel.State;

        /// <summary>
        /// gRPC Dgraph client.
        /// </summary>
        internal DgraphClient Client => _client;

        /// <summary>
        /// Closes the connection. Prefer <see cref="DgraphNetClient.CloseAsync"/> or <see cref="DgraphConnectionPool.CloseAllAsync"/> instead.
        /// </summary>
        /// <returns></returns>
        public Task CloseAsync()
        {
            return _channel.ShutdownAsync();
        }
    }
}
