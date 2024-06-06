using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SNI;
using Microsoft.Data.SqlClient.SqlClientX.Providers.Transport;

namespace Microsoft.Data.SqlClient.SqlClientX.Handlers.ConnectionHandler
{
    /// <summary>
    ///  A handler to parse data sources and extract information from it.
    /// </summary>
    internal class DataSourceHandler : Handler
    {

        public override async ValueTask Handle(HandlerRequest request, bool isAsync, CancellationToken ct)
        {
            if (CanHandle(request))
            {
                ConnectionRequest connectionRequest = (ConnectionRequest)request;
                string dataSource = connectionRequest.ConnectionStringBuilder.DataSource;
                // Parse the connection string and extract the data source
                // For now, we are just returning the data source as it is.
                DataSource details = Microsoft.Data.SqlClient.SNI.DataSource.ParseServerName(dataSource);
                connectionRequest.DataSource = details;
            }

            // Invoke the next handler 
            if (NextHandler != null)
            {
                await NextHandler.Handle(request, isAsync, timeout, ct).ConfigureAwait(false);
            }
        }

        internal bool CanHandle(HandlerRequest request) => request.RequestType == HandlerRequestType.ConnectionRequest;

    }

    internal class TransportHandler : Handler
    {
        /// <summary>
        /// Creates the physical connection to the server.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="isAsync"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public override async ValueTask Handle(HandlerRequest request, bool isAsync, CancellationToken ct)
        {
            if (CanHandle(request))
            {
                var connectionRequest = (ConnectionRequest)request;
                var dataSource = connectionRequest.DataSource;
                DataSource.Protocol protocol = dataSource._connectionProtocol;
                int timeout = connectionRequest.ConnectionStringBuilder.ConnectTimeout;
                TransportProvider provider = new TransportProvider(protocol, timeout);
                Stream transportStream = await provider.CreateTransport(isAsync, ct).ConfigureAwait(false);
                connectionRequest.TransportStream = transportStream;


            }

            // Invoke the next handler 
            if (NextHandler != null)
            {
                await NextHandler.Handle(request, isAsync, ct).ConfigureAwait(false);
            }
        }

        internal bool CanHandle(HandlerRequest request) => request.RequestType == HandlerRequestType.ConnectionRequest && (request as ConnectionRequest).DataSource != null;

    }
}
