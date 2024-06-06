using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SNI;

namespace Microsoft.Data.SqlClient.SqlClientX.Handlers
{
    internal interface IHandler
    {
        public abstract ValueTask Handle(HandlerRequest request, bool isAsync, CancellationToken ct);

        public void SetNext(IHandler handler);
    }

    internal abstract class Handler : IHandler
    {
        private IHandler _nextHandler;
        
        public IHandler NextHandler { get => this._nextHandler; set => _nextHandler = value; }

        public abstract ValueTask Handle(HandlerRequest request, bool isAsync, CancellationToken ct);

        public virtual void SetNext(IHandler handler)
        {
            NextHandler = handler;
        }
    }

    internal enum HandlerRequestType
    {
        ConnectionRequest
    }
    internal abstract class HandlerRequest
    {
        public HandlerRequestType RequestType { get; internal set; }
        public Exception Exception { get; set; }

    }

    internal class ConnectionRequest : HandlerRequest
    {
        public string ConnectionString { get; set; }

        public SqlConnectionStringBuilder ConnectionStringBuilder { get; internal set; }
        public DataSource DataSource { get; internal set; }

        public ConnectionRequest(string connectionString)
        {
            RequestType = HandlerRequestType.ConnectionRequest;
            ConnectionString = connectionString;
            ConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
        }
    } 
}
