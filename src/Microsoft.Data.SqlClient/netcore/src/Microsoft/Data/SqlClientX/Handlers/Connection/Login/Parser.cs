// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClientX.IO;

namespace Microsoft.Data.SqlClientX.Handlers.Connection.Login
{
    internal abstract class Parser
    {
        internal async ValueTask<int> GetTokenLength(Tokens tdsToken, TdsStream tdsStream, bool isAsync, CancellationToken ct)
        {
            byte token = ((byte)tdsToken);
            int tokenLength;

            switch (token)
            { // rules about SQLLenMask no longer apply to new tokens (as of 7.4)
                case TdsEnums.SQLFEATUREEXTACK:
                    tokenLength = -1;
                    break;
                case TdsEnums.SQLSESSIONSTATE:
                    tokenLength = await tdsStream.TdsReader.ReadInt32Async(isAsync, ct).ConfigureAwait(false);
                    break;
                case TdsEnums.SQLFEDAUTHINFO:
                    tokenLength = await tdsStream.TdsReader.ReadInt32Async(isAsync, ct).ConfigureAwait(false);
                    break;
                default:
                    // Use Token mask based switching.
                    switch (token & TdsEnums.SQLLenMask)
                    {
                        case TdsEnums.SQLFixedLen:
                            tokenLength = ((0x01 << ((token & 0x0c) >> 2))) & 0xff;
                            break;
                        case TdsEnums.SQLZeroLen:
                            tokenLength = 0;
                            break;
                        case TdsEnums.SQLVarLen:
                        case TdsEnums.SQLVarCnt:
                            if (0 != (token & 0x80))
                            {
                                tokenLength = await tdsStream.TdsReader.ReadUInt16Async(isAsync, ct).ConfigureAwait(false);
                                break;
                            }
                            else if (0 == (token & 0x0c))
                            {
                                tokenLength = await tdsStream.TdsReader.ReadInt32Async(isAsync, ct).ConfigureAwait(false);
                                break;
                            }
                            else
                            {
                                tokenLength = await tdsStream.TdsReader.ReadByteAsync(isAsync, ct).ConfigureAwait(false);
                                break;
                            }
                        default:
                            Debug.Fail("Unknown token length!");
                            tokenLength = 0;
                            break;
                    }
                    break;
            }
            return tokenLength;
        }

        internal abstract Task Parse(TdsStream tdsStream, ILoginTokenListener listener, bool isAsync, CancellationToken ct);
    }
}
