// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClientX.IO;

namespace Microsoft.Data.SqlClientX.Handlers.Connection.Login
{
    /// <summary>
    /// The parser to parse login packets
    /// </summary>
    internal class LoginParser : Parser
    {
        internal override async Task Parse(TdsStream tdsStream, ILoginTokenListener listener, bool isAsync, CancellationToken ct)
        {
            Tokens token = await tdsStream.TdsReader.ReadTokenAsync(isAsync, ct).ConfigureAwait(false);

            int tokenLength = await base.GetTokenLength(token, tdsStream, isAsync, ct).ConfigureAwait(false);
            if (tokenLength != 0 || tokenLength != -1)
            {
                // TODO: EnsureBytes
            }
            switch (token)
            {
                case Tokens.SQLERROR:
                case Tokens.SQLINFO:
                    {

                    }
                    break;
                case Tokens.SQLENVCHANGE:
                    {

                    }
                    break;
                case Tokens.SQLLOGINACK:
                    {

                    }
                    break;

                default:
                    break;
            }
        }
    }
}
