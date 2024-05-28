using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SqlClientX.Streams;

namespace Microsoft.Data.SqlClient.SqlClientX.ProtocolHandlers
{
    internal abstract class TdsHandler
    {

        abstract public RequestType SupportedRequest { get; }

        public bool CanHandle(RequestType requestType) => SupportedRequest == requestType;

        internal TdsHandler(TdsReadStream readStream,
            TdsWriteStream writeStream)
        {
            this.ReadStream = readStream;
            this.WriteStream = writeStream;
        }

        public TdsReadStream ReadStream { get; private set; }
        public TdsWriteStream WriteStream { get; private set; }
        internal TdsHandler Next { get; set; }

        internal HandlerContext Context { get; }

        internal async ValueTask Handle(RequestType requestType,
            HandlerContext context,
            bool isAsync,
            CancellationToken ct)
        {
            if (CanHandle(requestType))
            {
                await HandleCore(requestType, context, isAsync, ct);
            }
        }

        internal abstract ValueTask HandleCore(RequestType requestType,
            HandlerContext context,
            bool isAsync,
            CancellationToken ct);
    }

    internal class HandlerContext
    {
    }

    internal enum RequestType
    {
        PreLogin,
        Login,
        Mars
    }
}
