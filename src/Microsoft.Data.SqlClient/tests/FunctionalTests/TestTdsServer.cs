// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.SqlServer.TDS.EndPoint;
using Microsoft.SqlServer.TDS.Servers;
using Newtonsoft.Json.Serialization;

namespace Microsoft.Data.SqlClient.Tests
{
    internal class TestTdsServerParameters
    {
        private const int DefaultConnectionTimeout = 5;

        internal bool EnabledFedAuth { get; set; }
        internal bool EnableLog { get; set; }
        internal int ConnectionTimeout { get; set; }

        internal bool ExcludeEncryption { get; set; }

        public TestTdsServerParameters()
        {
            ConnectionTimeout = DefaultConnectionTimeout;
            EnabledFedAuth = false;
            EnableLog = false;
            ExcludeEncryption = false;
        }
    }

    internal class TestTdsServer : GenericTDSServer, IDisposable
    {
        private TDSServerEndPoint _endpoint = null;

        private SqlConnectionStringBuilder _connectionStringBuilder;

        public TestTdsServer(TDSServerArguments args) : base(args) { }

        public TestTdsServer(QueryEngine engine, TDSServerArguments args) : base(args)
        {
            Engine = engine;
        }

        public static TestTdsServer StartServerWithQueryEngine(QueryEngine engine, TestTdsServerParameters parameters, [CallerMemberName] string methodName = "")
        {
            TDSServerArguments args = new TDSServerArguments()
            {
                Log = parameters.EnableLog ? Console.Out : null,
            };

            if (parameters.EnabledFedAuth)
            {
                args.FedAuthRequiredPreLoginOption = SqlServer.TDS.PreLogin.TdsPreLoginFedAuthRequiredOption.FedAuthRequired;
            }
            if (parameters.ExcludeEncryption)
            {
                args.Encryption = SqlServer.TDS.PreLogin.TDSPreLoginTokenEncryptionType.None;
            }

            TestTdsServer server = engine == null ? new TestTdsServer(args) : new TestTdsServer(engine, args);
            server._endpoint = new TDSServerEndPoint(server) { ServerEndPoint = new IPEndPoint(IPAddress.Any, 0) };
            server._endpoint.EndpointName = methodName;
            // The server EventLog should be enabled as it logs the exceptions.
            server._endpoint.EventLog = Console.Out;
            server._endpoint.Start();

            int port = server._endpoint.ServerEndPoint.Port;
            // Allow encryption to be set when encryption is to be excluded from pre-login response.
            SqlConnectionEncryptOption encryptionOption = parameters.ExcludeEncryption ? SqlConnectionEncryptOption.Mandatory : SqlConnectionEncryptOption.Optional;
            server._connectionStringBuilder = new SqlConnectionStringBuilder() { DataSource = "localhost," + port, ConnectTimeout = parameters.ConnectionTimeout, Encrypt = encryptionOption };
            server.ConnectionString = server._connectionStringBuilder.ConnectionString;
            return server;
        }

        public static TestTdsServer StartTestServer(TestTdsServerParameters parameters, [CallerMemberName] string methodName = "")
        {
            return StartServerWithQueryEngine(null, parameters, methodName);
        }

        public void Dispose() => _endpoint?.Stop();

        public string ConnectionString { get; private set; }
    }
}
