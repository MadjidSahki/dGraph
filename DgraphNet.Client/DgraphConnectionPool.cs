using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using static DgraphNet.Client.Proto.Dgraph;

namespace DgraphNet.Client
{
    /// <summary>
    /// Manage a pool of Dgraph connections.
    /// <para/>Connections must belong to the same Dgraph cluster.
    /// </summary>
    public class DgraphConnectionPool
    {
        List<DgraphConnection> _connections;

        /// <summary>
        /// Creates an empty <see cref="DgraphConnectionPool"/>.
        /// </summary>
        public DgraphConnectionPool()
        {
            _connections = new List<DgraphConnection>();
        }

        /// <summary>
        /// Creates a new <see cref="DgraphConnectionPool"/> from a list of <see cref="Channel"/>.
        /// <para/>For each channel, a <see cref="DgraphConnection"/> is created.
        /// </summary>
        /// <param name="channels">A list of gRPC channels.</param>
        public DgraphConnectionPool(IEnumerable<Channel> channels) : this()
        {
            Add(channels);
        }

        /// <summary>
        /// Creates a new <see cref="DgraphConnectionPool"/> from a list of <see cref="DgraphConnection"/>.
        /// </summary>
        /// <param name="connections">A list of <see cref="DgraphConnection"/>.</param>
        public DgraphConnectionPool(IEnumerable<DgraphConnection> connections) : this()
        {
            Add(connections);
        }

        /// <summary>
        /// Connections of the pool.
        /// </summary>
        public IReadOnlyList<DgraphConnection> Connections => _connections;

        /// <summary>
        /// Adds a new channel to the pool. A new <see cref="DgraphConnection"/> will be created.
        /// </summary>
        /// <param name="channel">The channel to add.</param>
        /// <returns></returns>
        public DgraphConnectionPool Add(Channel channel)
        {
            var conn = new DgraphConnection(channel);
            _connections.Add(conn);
            return this;
        }

        /// <summary>
        /// Adds new channels to the pool. A new <see cref="DgraphConnection"/> will be created for each channel.
        /// </summary>
        /// <param name="channels">The channels to add.</param>
        /// <returns></returns>
        public DgraphConnectionPool Add(IEnumerable<Channel> channels)
        {
            foreach (var c in channels) Add(c);
            return this;
        }

        /// <summary>
        /// Add a new connection to the pool.
        /// </summary>
        /// <param name="connection">The connection to add.</param>
        /// <returns></returns>
        public DgraphConnectionPool Add(DgraphConnection connection)
        {
            _connections.Add(connection);
            return this;
        }

        /// <summary>
        /// Add new connections to the pool.
        /// </summary>
        /// <param name="connections">The connections to add.</param>
        /// <returns></returns>
        public DgraphConnectionPool Add(IEnumerable<DgraphConnection> connections)
        {
            foreach (var c in connections) Add(c);
            return this;
        }

        /// <summary>
        /// Closes the connections.
        /// You can instead call <see cref="DgraphNetClient.CloseAsync"/>.
        /// </summary>
        /// <returns></returns>
        public async Task CloseAllAsync()
        {
            foreach (var connection in _connections)
            {
                await connection.CloseAsync();
            }
        }
    }
}
