﻿using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.SqlClientX.Streams;

namespace simplesqlclient
{
    internal class Utilities
    {
        private const int NoProcessId = -1;

        private static int s_currentProcessId = NoProcessId;

        internal static int GetCurrentProcessIdForTdsLoginOnly()
        {
            if (s_currentProcessId == NoProcessId)
            {
                // Pick up the process Id from the current process instead of randomly generating it.
                // This would be helpful while tracing application related issues.
                int processId;
                using (System.Diagnostics.Process p = System.Diagnostics.Process.GetCurrentProcess())
                {
                    processId = p.Id;
                }
                System.Threading.Volatile.Write(ref s_currentProcessId, processId);
            }
            return s_currentProcessId;
        }

        internal static byte[] ObfuscatePassword(string password)
        {
            byte[] bObfuscated = new byte[password.Length << 1];
            int s;
            byte bLo;
            byte bHi;

            for (int i = 0; i < password.Length; i++)
            {
                s = (int)password[i];
                bLo = (byte)(s & 0xff);
                bHi = (byte)((s >> 8) & 0xff);
                bObfuscated[i << 1] = (byte)((((bLo & 0x0f) << 4) | (bLo >> 4)) ^ 0xa5);
                bObfuscated[(i << 1) + 1] = (byte)((((bHi & 0x0f) << 4) | (bHi >> 4)) ^ 0xa5);
            }
            return bObfuscated;
        }

        internal static bool IsVarTimeTds(byte tdsType) => tdsType == LucidTdsEnums.SQLTIME || tdsType == LucidTdsEnums.SQLDATETIME2 || tdsType == LucidTdsEnums.SQLDATETIMEOFFSET;

        internal static async ValueTask<int> GetSpecialTokenLengthAsync(byte tokenType,
            TdsReadStream stream,
            bool isAsync,
            CancellationToken ct)
        {
            bool specialToken = false;
            int length = 0;
            switch (tokenType)
            {
                // Handle special tokens.
                case TdsTokens.SQLFEATUREEXTACK:
                    length = -1;
                    specialToken = true;
                    break;
                case TdsTokens.SQLSESSIONSTATE:
                    length = await stream.ReadInt32Async(
                        isAsync, 
                        ct).ConfigureAwait(false);
                    specialToken = true;
                    break;
                case TdsTokens.SQLFEDAUTHINFO:
                    length = await stream.ReadInt32Async(
                        isAsync, 
                        ct).ConfigureAwait(false);
                    specialToken = true;
                    break;
                case TdsTokens.SQLUDT:
                case TdsTokens.SQLRETURNVALUE:
                    length = -1;
                    specialToken = true;
                    break;
                case TdsTokens.SQLXMLTYPE:
                    length = await stream.ReadUInt16Async(
                        isAsync, 
                        ct).ConfigureAwait(false);
                    specialToken = true;
                    break;

                default:
                    specialToken = false;
                    break;
            }

            int tokenLength = 0;
            if (!specialToken)
            {
                switch (tokenType & LucidTdsEnums.SQLLenMask)
                {
                    case LucidTdsEnums.SQLFixedLen:
                        tokenLength = (0x01 << ((tokenType & 0x0c) >> 2)) & 0xff;
                        break;
                    case LucidTdsEnums.SQLZeroLen:
                        tokenLength = 0;
                        break;
                    case LucidTdsEnums.SQLVarLen:
                    case LucidTdsEnums.SQLVarCnt:
                        if (0 != (tokenType & 0x80))
                        {
                            tokenLength = await stream.ReadUInt16Async(
                                isAsync, 
                                ct).ConfigureAwait(false);
                            break;
                        }
                        else if (0 == (tokenType & 0x0c))
                        {
                            tokenLength = await stream.ReadInt32Async(
                                isAsync, 
                                ct).ConfigureAwait(false);
                            break;
                        }
                        else
                        {
                            byte value = await stream.ReadByteAsync(
                                isAsync, 
                                ct).ConfigureAwait(false);
                            break;
                        }
                    default:
                        Debug.Fail("Unknown token length!");
                        tokenLength = 0;
                        break;
                }
                length = tokenLength;
            }
            // Read the length

            // Read the data

            return length;
        }

        internal static bool IsNull(MetaType mt, int length)
        {
            // null bin and char types have a length of -1 to represent null
            if (mt.IsPlp)
            {
                return (LucidTdsEnums.SQL_PLP_NULL == (ulong)length);
            }

            // HOTFIX #50000415: for image/text, 0xFFFF is the length, not representing null
            if ((LucidTdsEnums.VARNULL == length) && !mt.IsLong)
            {
                return true;
            }

            // other types have a length of 0 to represent null
            // long and non-PLP types will always return false because these types are either char or binary
            // this is expected since for long and non-plp types isnull is checked based on textptr field and not the length
            return ((LucidTdsEnums.FIXEDNULL == length) && !mt.IsCharType && !mt.IsBinType);
        }
    }


}
