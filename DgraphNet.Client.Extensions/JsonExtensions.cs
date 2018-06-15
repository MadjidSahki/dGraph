using Google.Protobuf;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static DgraphNet.Client.DgraphNetClient;

namespace DgraphNet.Client.Extensions
{
    public static class JsonExtensions
    {
        private static T Deserialize<T>(ByteString byteString) where T : class
        {
            using (var stream = new MemoryStream(byteString.ToByteArray()))
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                return JsonSerializer.Create().Deserialize(reader, typeof(T)) as T;
            }
        }

        #region Query

        public static T Query<T>(this Transaction @this, string query)
            where T : class
        {
            var resp = @this.Query(query);
            return Deserialize<T>(resp.Json);
        }

        public static async Task<T> QueryAsync<T>(this Transaction @this, string query, CancellationToken cancellationToken = default(CancellationToken))
            where T : class
        {
            var resp = await @this.QueryAsync(query);
            return Deserialize<T>(resp.Json);
        }

        #endregion

        #region QueryWithVars

        public static T QueryWithVars<T>(this Transaction @this, string query, IDictionary<string, string> vars)
            where T : class
        {
            var resp = @this.QueryWithVars(query, vars);
            return Deserialize<T>(resp.Json);
        }

        public static async Task<T> QueryWithVarsAsync<T>(this Transaction @this, string query, IDictionary<string, string> vars, CancellationToken cancellationToken = default(CancellationToken))
            where T : class
        {
            var resp = await @this.QueryWithVarsAsync(query, vars);
            return Deserialize<T>(resp.Json);
        }

        #endregion
    }
}
