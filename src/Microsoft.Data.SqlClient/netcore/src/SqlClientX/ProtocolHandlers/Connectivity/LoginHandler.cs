using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SqlClientX.Streams;

namespace Microsoft.Data.SqlClient.SqlClientX.ProtocolHandlers.Connectivity
{
    internal class PreLoginHandler : TdsHandler
    {
        public PreLoginHandler(TdsReadStream readStream,
            TdsWriteStream writeStream) : base(readStream, writeStream)
        {
            this.Next = new LoginHandler(readStream, writeStream);
        }

        public override RequestType SupportedRequest => RequestType.PreLogin;

        internal override ValueTask HandleCore(RequestType requestType, HandlerContext context, bool isAsync, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }


    internal class LoginHandler : TdsHandler
    {
        public override RequestType SupportedRequest => RequestType.Login;

        public LoginHandler(TdsReadStream readStream, TdsWriteStream writeStream) : base(readStream, writeStream)
        {
        }

        internal override ValueTask HandleCore(RequestType requestType, HandlerContext context, bool isAsync, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }

    internal class MarsHandler : TdsHandler
    {
        public override RequestType SupportedRequest => RequestType.Mars;

        public MarsHandler(
            TdsReadStream readStream,
            TdsWriteStream writeStream) 
            : base(
                  readStream, 
                  writeStream)
        {
        }

        internal override ValueTask HandleCore(RequestType requestType, HandlerContext context, bool isAsync, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
