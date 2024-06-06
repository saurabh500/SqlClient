using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SNI;

namespace Microsoft.Data.SqlClient.SqlClientX.Providers.Transport
{
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
}
