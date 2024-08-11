// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
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
            TdsToken token;

            do
            {
                token = await tdsStream.TdsReader.ReadTokenAsync(isAsync, ct).ConfigureAwait(false);
                int tokenLength = await base.GetTokenLength(token, tdsStream, isAsync, ct).ConfigureAwait(false);
            
                if (tokenLength != 0 || tokenLength != -1)
                {
                    // TODO: EnsureBytes
                }

            
                switch (token)
                {
                    case TdsToken.SQLERROR:
                    case TdsToken.SQLINFO:
                        {
                            int errorNumber = await tdsStream.TdsReader.ReadInt32Async(isAsync, ct).ConfigureAwait(false);
                            byte errorState = await tdsStream.TdsReader.ReadByteAsync(isAsync, ct).ConfigureAwait(false);
                            byte errorClass = await tdsStream.TdsReader.ReadByteAsync(isAsync, ct).ConfigureAwait(false);
                            ushort len = await tdsStream.TdsReader.ReadUInt16Async(isAsync, ct).ConfigureAwait(false);
                            string message = await tdsStream.TdsReader.ReadStringAsync(len, isAsync, ct).ConfigureAwait(false);
                            byte serverLength = await tdsStream.TdsReader.ReadByteAsync(isAsync, ct).ConfigureAwait(false);
                            string server = await tdsStream.TdsReader.ReadStringAsync(serverLength, isAsync, ct).ConfigureAwait(false);
                            byte procLength = await tdsStream.TdsReader.ReadByteAsync(isAsync, ct).ConfigureAwait(false);
                            string proc = await tdsStream.TdsReader.ReadStringAsync(procLength, isAsync, ct).ConfigureAwait(false);
                            int lineNumber = await tdsStream.TdsReader.ReadInt32Async(isAsync, ct).ConfigureAwait(false);
                            TdsError error = new TdsError(errorNumber, errorState, errorClass, message, server, proc, lineNumber);
                            listener?.AccumulateError(error);
                        }
                        break;
                    case TdsToken.SQLENVCHANGE:
                        {
                            // Initialize a memory of tokenLength
                            Memory<byte> buffer = new byte[tokenLength];
                            // Do nothing
                            await tdsStream.TdsReader.ReadBytesAsync(buffer, isAsync, ct).ConfigureAwait(false);

                        }
                        break;
                    case TdsToken.SQLLOGINACK:
                        {
                            Memory<byte> buffer = new byte[tokenLength];
                            // Do nothing
                            await tdsStream.TdsReader.ReadBytesAsync(buffer, isAsync, ct).ConfigureAwait(false);
                        }
                        break;
                    case TdsToken.SQLDONE:
                        {
                            Memory<byte> buffer = new byte[tokenLength];
                            // Do nothing
                            await tdsStream.TdsReader.ReadBytesAsync(buffer, isAsync, ct).ConfigureAwait(false);
                        }
                        break;
                    case TdsToken.SQLFEATUREEXTACK:
                        {
                            TdsFeature feature;
                            do
                            {
                                feature = await tdsStream.TdsReader.ReadFeatureAsync(isAsync, ct).ConfigureAwait(false);
                                if (feature == TdsFeature.FEATUREEXT_TERMINATOR)
                                {
                                    break;
                                }
                                uint dataLength = await tdsStream.TdsReader.ReadUInt32Async(isAsync, ct).ConfigureAwait(false);
                                Memory<byte> buffer = new byte[dataLength];
                                // Do nothing
                                await tdsStream.TdsReader.ReadBytesAsync(buffer, isAsync, ct).ConfigureAwait(false);

                            } while (feature != TdsFeature.FEATUREEXT_TERMINATOR);

                        }
                        break;
                    default:
                        { 
                            Memory<byte> buffer = new byte[tokenLength];
                            // Do nothing
                            await tdsStream.TdsReader.ReadBytesAsync(buffer, isAsync, ct).ConfigureAwait(false);
                        }
                        break;
                }
            } while (token != TdsToken.SQLDONE);
        }
    }
}
