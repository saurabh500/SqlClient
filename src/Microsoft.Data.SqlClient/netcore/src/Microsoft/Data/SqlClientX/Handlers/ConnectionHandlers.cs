using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.SNI;
using Microsoft.Data.SqlClientX.IO;

namespace Microsoft.Data.SqlClientX.Handlers
{
    internal class TransportHandler : Handler<ConnectionRequest>
    {
        /// <summary>
        /// Creates the physical connection to the server.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="isAsync"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public override async ValueTask Handle(ConnectionRequest request, bool isAsync, CancellationToken ct)
        {
            if (CanHandle(request))
            {
                var connectionRequest = (ConnectionRequest)request;
                var dataSource = connectionRequest.DataSource;
                DataSource.Protocol protocol = dataSource._connectionProtocol;
                int timeout = connectionRequest.ConnectionStringBuilder.ConnectTimeout;
                TransportProvider provider = new TransportProvider(protocol);
                Stream transportStream = await provider.CreateTransport(isAsync, ct).ConfigureAwait(false);
                connectionRequest.TransportStream = transportStream;
            }

            // Invoke the next handler 
            if (NextHandler != null)
            {
                await NextHandler.Handle(request, isAsync, ct).ConfigureAwait(false);
            }
        }

        internal bool CanHandle(ConnectionRequest request) => request.DataSource != null;

    }

    internal class PreloginHandler : Handler<ConnectionRequest>
    {
        private TdsStream _tdsStream;

        /// <summary>
        /// Creates the physical connection to the server.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="isAsync"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public override async ValueTask Handle(ConnectionRequest request, bool isAsync, CancellationToken ct)
        {
            if (CanHandle(request))
            {
                ConnectionRequest connectionRequest = request as ConnectionRequest;
                Stream transportStream = connectionRequest.TransportStream;
                // Send prelogin
                TdsStream tdsStream = new TdsStream(transportStream);
                connectionRequest.TdsStream = tdsStream;
                this._tdsStream = tdsStream;
                await Send(isAsync, ct).ConfigureAwait(false);
                await Consume(isAsync, ct).ConfigureAwait(false);
            }

            // Invoke the next handler 
            if (NextHandler != null)
            {
                await NextHandler.Handle(request, isAsync, ct).ConfigureAwait(false);
            }
        }

        public ValueTask Send(
            bool isAsync,
            CancellationToken ct)
        {
            return ValueTask.CompletedTask;
        }

        internal ValueTask<PreLoginResponse> Consume(bool isAsync, CancellationToken ct)
        {
            return ValueTask.FromResult(new PreLoginResponse());
        }

        internal bool CanHandle(ConnectionRequest request) =>
            request.TransportStream != null;

    }

    internal class TransportProvider
    {
        private DataSource.Protocol _protocol;

        public TransportProvider(DataSource.Protocol protocol)
        {
            this._protocol = protocol;
        }

        public ValueTask<Stream> CreateTransport(bool isAsync, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }

    internal class PreLoginResponse
    {
        
    }

}
