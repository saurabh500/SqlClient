// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Data.Common;
using Microsoft.Data.ProviderBase;
using Microsoft.Data.Sql;
using Microsoft.Data.SqlClient.DataClassification;
using Microsoft.Data.SqlClient.Server;
using Microsoft.Data.SqlTypes;

namespace Microsoft.Data.SqlClient
{

    internal partial class TdsParserExtensions
    {
        internal static void WriteInt(Span<byte> buffer, int value)
        {
#if NET6_0_OR_GREATER
            BinaryPrimitives.TryWriteInt32LittleEndian(buffer, value);
#else
            buffer[0] = (byte)(value & 0xff);
            buffer[1] = (byte)((value >> 8) & 0xff);
            buffer[2] = (byte)((value >> 16) & 0xff);
            buffer[3] = (byte)((value >> 24) & 0xff);
#endif
        }


        //
        // Takes a 16 bit short and writes it to the returned buffer.
        //
        internal static byte[] SerializeShort(int v, TdsParserStateObject stateObj)
        {
            if (null == stateObj._bShortBytes)
            {
                stateObj._bShortBytes = new byte[2];
            }
            else
            {
                Debug.Assert(2 == stateObj._bShortBytes.Length);
            }

            byte[] bytes = stateObj._bShortBytes;
            int current = 0;
            bytes[current++] = (byte)(v & 0xff);
            bytes[current++] = (byte)((v >> 8) & 0xff);
            return bytes;
        }

        //
        // Takes a 16 bit short and writes it.
        //
        internal static void WriteShort(int v, TdsParserStateObject stateObj)
        {
            if ((stateObj._outBytesUsed + 2) > stateObj._outBuff.Length)
            {
                // if all of the short doesn't fit into the buffer
                stateObj.WriteByte((byte)(v & 0xff));
                stateObj.WriteByte((byte)((v >> 8) & 0xff));
            }
            else
            {
                // all of the short fits into the buffer
                stateObj._outBuff[stateObj._outBytesUsed] = (byte)(v & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 1] = (byte)((v >> 8) & 0xff);
                stateObj._outBytesUsed += 2;
            }
        }

        internal static void WriteUnsignedShort(ushort us, TdsParserStateObject stateObj)
        {
            WriteShort((short)us, stateObj);
        }

        //
        // Takes a long and writes out an unsigned int
        //
        internal static byte[] SerializeUnsignedInt(uint i, TdsParserStateObject stateObj)
        {
            return SerializeInt((int)i, stateObj);
        }

        internal static void WriteUnsignedInt(uint i, TdsParserStateObject stateObj)
        {
            WriteInt((int)i, stateObj);
        }

        //
        // Takes an int and writes it as an int.
        //
        internal static byte[] SerializeInt(int v, TdsParserStateObject stateObj)
        {
            if (null == stateObj._bIntBytes)
            {
                stateObj._bIntBytes = new byte[sizeof(int)];
            }
            else
            {
                Debug.Assert(sizeof(int) == stateObj._bIntBytes.Length);
            }

            WriteInt(stateObj._bIntBytes.AsSpan(), v);
            return stateObj._bIntBytes;
        }

        internal static void WriteInt(int v, TdsParserStateObject stateObj)
        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];
            WriteInt(buffer, v);
            if ((stateObj._outBytesUsed + 4) > stateObj._outBuff.Length)
            {
                // if all of the int doesn't fit into the buffer
                for (int index = 0; index < sizeof(int); index++)
                {
                    stateObj.WriteByte(buffer[index]);
                }
            }
            else
            {
                // all of the int fits into the buffer
                buffer.CopyTo(stateObj._outBuff.AsSpan(stateObj._outBytesUsed, sizeof(int)));
                stateObj._outBytesUsed += 4;
            }
        }

        //
        // Takes a float and writes it as a 32 bit float.
        //
        internal static byte[] SerializeFloat(float v)
        {
            if (Single.IsInfinity(v) || Single.IsNaN(v))
            {
                throw ADP.ParameterValueOutOfRange(v.ToString());
            }

            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, BitConverterCompatible.SingleToInt32Bits(v));
            return bytes;
        }

        internal static void WriteFloat(float v, TdsParserStateObject stateObj)
        {
            Span<byte> bytes = stackalloc byte[sizeof(float)];
            FillFloatBytes(v, bytes);
            stateObj.WriteByteSpan(bytes);
        }
        internal static void FillFloatBytes(float value, Span<byte> buffer) => BinaryPrimitives.TryWriteInt32LittleEndian(buffer, BitConverterCompatible.SingleToInt32Bits(value));

        //
        // Takes a long and writes it as a long.
        //
        internal static byte[] SerializeLong(long v, TdsParserStateObject stateObj)
        {
            int current = 0;
            if (null == stateObj._bLongBytes)
            {
                stateObj._bLongBytes = new byte[8];
            }

            byte[] bytes = stateObj._bLongBytes;
            Debug.Assert(8 == bytes.Length, "Cached buffer has wrong size");

            bytes[current++] = (byte)(v & 0xff);
            bytes[current++] = (byte)((v >> 8) & 0xff);
            bytes[current++] = (byte)((v >> 16) & 0xff);
            bytes[current++] = (byte)((v >> 24) & 0xff);
            bytes[current++] = (byte)((v >> 32) & 0xff);
            bytes[current++] = (byte)((v >> 40) & 0xff);
            bytes[current++] = (byte)((v >> 48) & 0xff);
            bytes[current++] = (byte)((v >> 56) & 0xff);

            return bytes;
        }

        internal static void WriteLong(long v, TdsParserStateObject stateObj)
        {
            if ((stateObj._outBytesUsed + 8) > stateObj._outBuff.Length)
            {
                // if all of the long doesn't fit into the buffer
                for (int shiftValue = 0; shiftValue < sizeof(long) * 8; shiftValue += 8)
                {
                    stateObj.WriteByte((byte)((v >> shiftValue) & 0xff));
                }
            }
            else
            {
                // all of the long fits into the buffer
                // NOTE: We don't use a loop here for performance
                stateObj._outBuff[stateObj._outBytesUsed] = (byte)(v & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 1] = (byte)((v >> 8) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 2] = (byte)((v >> 16) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 3] = (byte)((v >> 24) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 4] = (byte)((v >> 32) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 5] = (byte)((v >> 40) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 6] = (byte)((v >> 48) & 0xff);
                stateObj._outBuff[stateObj._outBytesUsed + 7] = (byte)((v >> 56) & 0xff);
                stateObj._outBytesUsed += 8;
            }
        }

        //
        // Takes a long and writes part of it
        //
        internal static byte[] SerializePartialLong(long v, int length)
        {
            Debug.Assert(length <= 8, "Length specified is longer than the size of a long");
            Debug.Assert(length >= 0, "Length should not be negative");

            byte[] bytes = new byte[length];

            // all of the long fits into the buffer
            for (int index = 0; index < length; index++)
            {
                bytes[index] = (byte)((v >> (index * 8)) & 0xff);
            }

            return bytes;
        }

        internal static void WritePartialLong(long v, int length, TdsParserStateObject stateObj)
        {
            Debug.Assert(length <= 8, "Length specified is longer than the size of a long");
            Debug.Assert(length >= 0, "Length should not be negative");

            if ((stateObj._outBytesUsed + length) > stateObj._outBuff.Length)
            {
                // if all of the long doesn't fit into the buffer
                for (int shiftValue = 0; shiftValue < length * 8; shiftValue += 8)
                {
                    stateObj.WriteByte((byte)((v >> shiftValue) & 0xff));
                }
            }
            else
            {
                // all of the long fits into the buffer
                for (int index = 0; index < length; index++)
                {
                    stateObj._outBuff[stateObj._outBytesUsed + index] = (byte)((v >> (index * 8)) & 0xff);
                }
                stateObj._outBytesUsed += length;
            }
        }

        //
        // Takes a ulong and writes it as a ulong.
        //
        internal static void WriteUnsignedLong(ulong uv, TdsParserStateObject stateObj)
        {
            WriteLong((long)uv, stateObj);
        }

        //
        // Takes a double and writes it as a 64 bit double.
        //
        internal static byte[] SerializeDouble(double v)
        {
            if (double.IsInfinity(v) || double.IsNaN(v))
            {
                throw ADP.ParameterValueOutOfRange(v.ToString());
            }

            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(bytes, BitConverter.DoubleToInt64Bits(v));
            return bytes;
        }

        internal static void WriteDouble(double v, TdsParserStateObject stateObj)
        {
            Span<byte> bytes = stackalloc byte[sizeof(double)];
            FillDoubleBytes(v, bytes);
            stateObj.WriteByteSpan(bytes);
        }

        internal static void FillDoubleBytes(double value, Span<byte> buffer) => BinaryPrimitives.TryWriteInt64LittleEndian(buffer, BitConverter.DoubleToInt64Bits(value));


        internal static bool TryReadTwoBinaryFields(SqlEnvChange env, TdsParserStateObject stateObj)
        {
            // Used by ProcessEnvChangeToken
            byte byteLength;
            if (!stateObj.TryReadByte(out byteLength))
            {
                return false;
            }
            env._newLength = byteLength;
            env._newBinValue = ArrayPool<byte>.Shared.Rent(env._newLength);
            env._newBinRented = true;
            if (!stateObj.TryReadByteArray(env._newBinValue, env._newLength))
            {
                return false;
            }
            if (!stateObj.TryReadByte(out byteLength))
            {
                return false;
            }
            env._oldLength = byteLength;
            env._oldBinValue = ArrayPool<byte>.Shared.Rent(env._oldLength);
            env._oldBinRented = true;
            if (!stateObj.TryReadByteArray(env._oldBinValue, env._oldLength))
            {
                return false;
            }

            // env.length includes 1 byte type token
            env._length = 3 + env._newLength + env._oldLength;
            return true;
        }

        internal static bool TryReadTwoStringFields(SqlEnvChange env, TdsParserStateObject stateObj)
        {
            // Used by ProcessEnvChangeToken
            byte newLength, oldLength;
            string newValue, oldValue;
            if (!stateObj.TryReadByte(out newLength))
            {
                return false;
            }
            if (!stateObj.TryReadString(newLength, out newValue))
            {
                return false;
            }
            if (!stateObj.TryReadByte(out oldLength))
            {
                return false;
            }
            if (!stateObj.TryReadString(oldLength, out oldValue))
            {
                return false;
            }

            env._newLength = newLength;
            env._newValue = newValue;
            env._oldLength = oldLength;
            env._oldValue = oldValue;

            // env.length includes 1 byte type token
            env._length = 3 + env._newLength * 2 + env._oldLength * 2;
            return true;
        }

        internal static Task WriteString(string s, int length, int offset, TdsParserStateObject stateObj, bool canAccumulate = true)
        {
            int cBytes = ADP.CharSize * length;

            // Perf shortcut: If it fits, write directly to the outBuff
            if (cBytes < (stateObj._outBuff.Length - stateObj._outBytesUsed))
            {
                CopyStringToBytes(s, offset, stateObj._outBuff, stateObj._outBytesUsed, length);
                stateObj._outBytesUsed += cBytes;
                return null;
            }
            else
            {
                if (stateObj._bTmp == null || stateObj._bTmp.Length < cBytes)
                {
                    stateObj._bTmp = new byte[cBytes];
                }

                CopyStringToBytes(s, offset, stateObj._bTmp, 0, length);
                return stateObj.WriteByteArray(stateObj._bTmp, cBytes, 0, canAccumulate);
            }
        }

        internal static void CopyStringToBytes(string source, int sourceOffset, byte[] dest, int destOffset, int charLength)
        {
            Encoding.Unicode.GetBytes(source, sourceOffset, charLength, dest, destOffset);
        }

        internal static byte[] SerializeString(string s, int length, int offset)
        {
            int cBytes = ADP.CharSize * length;
            byte[] bytes = new byte[cBytes];

            CopyStringToBytes(s, offset, bytes, 0, length);
            return bytes;
        }



        internal static void CopyCharsToBytes(char[] source, int sourceOffset, byte[] dest, int destOffset, int charLength)
        {
            if (!BitConverter.IsLittleEndian)
            {
                int desti = 0;
                Span<byte> span = dest.AsSpan();
                for (int srci = 0; srci < charLength; srci++)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(desti + destOffset), (ushort)source[srci + sourceOffset]);
                    desti += 2;
                }
            }
            else
            {
                Buffer.BlockCopy(source, sourceOffset, dest, destOffset, charLength * ADP.CharSize);
            }
        }



        internal static Task WriteEncodingChar(string s, Encoding encoding, TdsParserStateObject stateObj, Encoding defaultEncoding, bool canAccumulate = true)
        {
            return WriteEncodingChar(s, s.Length, 0, encoding, stateObj, defaultEncoding, canAccumulate);
        }

        internal static byte[] SerializeEncodingChar(string s, int numChars, int offset, Encoding encoding)
        {
#if NETFRAMEWORK
            char[] charData;
            byte[] byteData = null;

            // if hitting 7.0 server, encoding will be null in metadata for columns or return values since
            // 7.0 has no support for multiple code pages in data - single code page support only
            if (encoding == null)
                encoding = _defaultEncoding;

            charData = s.ToCharArray(offset, numChars);

            byteData = new byte[encoding.GetByteCount(charData, 0, charData.Length)];
            encoding.GetBytes(charData, 0, charData.Length, byteData, 0);

            return byteData;
#else
            return encoding.GetBytes(s, offset, numChars);
#endif
        }

        internal static Task WriteEncodingChar(string s, int numChars, int offset, Encoding encoding, TdsParserStateObject stateObj, Encoding defaultEncoding, bool canAccumulate = true)
        {
            // if hitting 7.0 server, encoding will be null in metadata for columns or return values since
            // 7.0 has no support for multiple code pages in data - single code page support only
            if (encoding == null)
                encoding = defaultEncoding;

            // Optimization: if the entire string fits in the current buffer, then copy it directly
            int bytesLeft = stateObj._outBuff.Length - stateObj._outBytesUsed;
            if ((numChars <= bytesLeft) && (encoding.GetMaxByteCount(numChars) <= bytesLeft))
            {
                int bytesWritten = encoding.GetBytes(s, offset, numChars, stateObj._outBuff, stateObj._outBytesUsed);
                stateObj._outBytesUsed += bytesWritten;
                return null;
            }
            else
            {
#if NETFRAMEWORK
                char[] charData = s.ToCharArray(offset, numChars);
                byte[] byteData = encoding.GetBytes(charData, 0, numChars);
                Debug.Assert(byteData != null, "no data from encoding");
                return stateObj.WriteByteArray(byteData, byteData.Length, 0, canAccumulate);
#else
                byte[] byteData = encoding.GetBytes(s, offset, numChars);
                Debug.Assert(byteData != null, "no data from encoding");
                return stateObj.WriteByteArray(byteData, byteData.Length, 0, canAccumulate);
#endif
            }
        }


        internal static byte[] SerializeDate(DateTime value)
        {
            long days = value.Subtract(DateTime.MinValue).Days;
            return SerializePartialLong(days, 3);
        }

        internal static void WriteDate(DateTime value, TdsParserStateObject stateObj)
        {
            long days = value.Subtract(DateTime.MinValue).Days;
            WritePartialLong(days, 3, stateObj);
        }

        internal static byte[] SerializeTime(TimeSpan value, byte scale, int length)
        {
            if (0 > value.Ticks || value.Ticks >= TimeSpan.TicksPerDay)
            {
                throw SQL.TimeOverflow(value.ToString());
            }

            long time = value.Ticks / TdsEnums.TICKS_FROM_SCALE[scale];

            // We normalize to maximum precision to allow conversion across different precisions.
            time = time * TdsEnums.TICKS_FROM_SCALE[scale];
            length = TdsEnums.MAX_TIME_LENGTH;

            return SerializePartialLong(time, length);
        }

        internal static void WriteTime(TimeSpan value, byte scale, int length, TdsParserStateObject stateObj)
        {
            if (0 > value.Ticks || value.Ticks >= TimeSpan.TicksPerDay)
            {
                throw SQL.TimeOverflow(value.ToString());
            }
            long time = value.Ticks / TdsEnums.TICKS_FROM_SCALE[scale];
            WritePartialLong(time, length, stateObj);
        }

        internal static byte[] SerializeDateTime2(DateTime value, byte scale, int length)
        {
            long time = value.TimeOfDay.Ticks / TdsEnums.TICKS_FROM_SCALE[scale]; // DateTime.TimeOfDay always returns a valid TimeSpan for Time

            // We normalize to maximum precision to allow conversion across different precisions.
            time = time * TdsEnums.TICKS_FROM_SCALE[scale];
            length = TdsEnums.MAX_DATETIME2_LENGTH;

            byte[] bytes = new byte[length];
            byte[] bytesPart;
            int current = 0;

            bytesPart = SerializePartialLong(time, length - 3);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, length - 3);
            current += length - 3;

            bytesPart = SerializeDate(value);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 3);

            return bytes;
        }

        internal static void WriteDateTime2(DateTime value, byte scale, int length, TdsParserStateObject stateObj)
        {
            long time = value.TimeOfDay.Ticks / TdsEnums.TICKS_FROM_SCALE[scale]; // DateTime.TimeOfDay always returns a valid TimeSpan for Time
            WritePartialLong(time, length - 3, stateObj);
            WriteDate(value, stateObj);
        }

        internal static byte[] SerializeDateTimeOffset(DateTimeOffset value, byte scale, int length)
        {
            byte[] bytesPart;
            int current = 0;

            bytesPart = SerializeDateTime2(value.UtcDateTime, scale, length - 2);

            // We need to allocate the array after we have received the length of the serialized value
            // since it might be higher due to normalization.
            length = bytesPart.Length + 2;
            byte[] bytes = new byte[length];

            Buffer.BlockCopy(bytesPart, 0, bytes, current, length - 2);
            current += length - 2;

            Int16 offset = (Int16)value.Offset.TotalMinutes;
            bytes[current++] = (byte)(offset & 0xff);
            bytes[current++] = (byte)((offset >> 8) & 0xff);

            return bytes;
        }

        internal static void WriteDateTimeOffset(DateTimeOffset value, byte scale, int length, TdsParserStateObject stateObj)
        {
            WriteDateTime2(value.UtcDateTime, scale, length - 2, stateObj);
            short offset = (short)value.Offset.TotalMinutes;
            stateObj.WriteByte((byte)(offset & 0xff));
            stateObj.WriteByte((byte)((offset >> 8) & 0xff));
        }

        internal static bool TryReadSqlDecimal(SqlBuffer value, int length, byte precision, byte scale, TdsParserStateObject stateObj)
        {
            byte byteValue;
            if (!stateObj.TryReadByte(out byteValue))
            {
                return false;
            }
            bool fPositive = (1 == byteValue);

            length = checked((int)length - 1);

            int[] bits;
            if (!TryReadDecimalBits(length, stateObj, out bits))
            {
                return false;
            }

            value.SetToDecimal(precision, scale, fPositive, bits);
            return true;
        }

        // @devnote: length should be size of decimal without the sign
        // @devnote: sign should have already been read off the wire
        internal static bool TryReadDecimalBits(int length, TdsParserStateObject stateObj, out int[] bits)
        {
            bits = stateObj._decimalBits; // used alloc'd array if we have one already
            int i;

            if (null == bits)
            {
                bits = new int[4];
                stateObj._decimalBits = bits;
            }
            else
            {
                for (i = 0; i < bits.Length; i++)
                    bits[i] = 0;
            }

            Debug.Assert((length > 0) &&
                         (length <= TdsEnums.MAX_NUMERIC_LEN - 1) &&
                         (length % 4 == 0), "decimal should have 4, 8, 12, or 16 bytes of data");

            int decLength = length >> 2;

            for (i = 0; i < decLength; i++)
            {
                // up to 16 bytes of data following the sign byte
                if (!stateObj.TryReadInt32(out bits[i]))
                {
                    return false;
                }
            }

            return true;
        }

        internal static SqlDecimal AdjustSqlDecimalScale(SqlDecimal d, int newScale)
        {
            if (d.Scale != newScale)
            {
                bool round = !TdsParser.EnableTruncateSwitch;
                return SqlDecimal.AdjustScale(d, newScale - d.Scale, round);
            }

            return d;
        }

        internal static decimal AdjustDecimalScale(decimal value, int newScale)
        {
            int oldScale = (decimal.GetBits(value)[3] & 0x00ff0000) >> 0x10;

            if (newScale != oldScale)
            {
                bool round = !TdsParser.EnableTruncateSwitch;
                SqlDecimal num = new SqlDecimal(value);
                num = SqlDecimal.AdjustScale(num, newScale - oldScale, round);
                return num.Value;
            }

            return value;
        }

        internal static byte[] SerializeSqlDecimal(SqlDecimal d, TdsParserStateObject stateObj)
        {
            if (null == stateObj._bDecimalBytes)
            {
                stateObj._bDecimalBytes = new byte[17];
            }

            byte[] bytes = stateObj._bDecimalBytes;
            int current = 0;

            // sign
            if (d.IsPositive)
                bytes[current++] = 1;
            else
                bytes[current++] = 0;


            Span<uint> data = stackalloc uint[4];
#if NET8_0_OR_GREATER
            d.WriteTdsValue(data);
#else
            SqlTypeWorkarounds.SqlDecimalExtractData(d, out data[0], out data[1], out data[2], out data[3]);
#endif
            byte[] bytesPart = SerializeUnsignedInt(data[0], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeUnsignedInt(data[1], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeUnsignedInt(data[2], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeUnsignedInt(data[3], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);

            return bytes;
        }

        internal static void WriteSqlDecimal(SqlDecimal d, TdsParserStateObject stateObj)
        {
            // sign
            if (d.IsPositive)
                stateObj.WriteByte(1);
            else
                stateObj.WriteByte(0);

            Span<uint> data = stackalloc uint[4];
#if NET8_0_OR_GREATER
            d.WriteTdsValue(data);
#else
            SqlTypeWorkarounds.SqlDecimalExtractData(d, out data[0], out data[1], out data[2], out data[3]);
#endif
            WriteUnsignedInt(data[0], stateObj);
            WriteUnsignedInt(data[1], stateObj);
            WriteUnsignedInt(data[2], stateObj);
            WriteUnsignedInt(data[3], stateObj);
        }

        internal static byte[] SerializeDecimal(decimal value, TdsParserStateObject stateObj)
        {
            int[] decimalBits = Decimal.GetBits(value);
            if (null == stateObj._bDecimalBytes)
            {
                stateObj._bDecimalBytes = new byte[17];
            }

            byte[] bytes = stateObj._bDecimalBytes;
            int current = 0;

            /*
             Returns a binary representation of a Decimal. The return value is an integer
             array with four elements. Elements 0, 1, and 2 contain the low, middle, and
             high 32 bits of the 96-bit integer part of the Decimal. Element 3 contains
             the scale factor and sign of the Decimal: bits 0-15 (the lower word) are
             unused; bits 16-23 contain a value between 0 and 28, indicating the power of
             10 to divide the 96-bit integer part by to produce the Decimal value; bits 24-
             30 are unused; and finally bit 31 indicates the sign of the Decimal value, 0
             meaning positive and 1 meaning negative.

             SQLDECIMAL/SQLNUMERIC has a byte stream of:
             struct {
                 BYTE sign; // 1 if positive, 0 if negative
                 BYTE data[];
             }

             For TDS 7.0 and above, there are always 17 bytes of data
            */

            // write the sign (note that COM and SQL are opposite)
            if (0x80000000 == (decimalBits[3] & 0x80000000))
                bytes[current++] = 0;
            else
                bytes[current++] = 1;

            byte[] bytesPart = SerializeInt(decimalBits[0], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeInt(decimalBits[1], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeInt(decimalBits[2], stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;
            bytesPart = SerializeInt(0, stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);

            return bytes;
        }

        internal static void WriteDecimal(decimal value, TdsParserStateObject stateObj)
        {
            stateObj._decimalBits = decimal.GetBits(value);
            Debug.Assert(null != stateObj._decimalBits, "decimalBits should be filled in at TdsExecuteRPC time");

            /*
             Returns a binary representation of a Decimal. The return value is an integer
             array with four elements. Elements 0, 1, and 2 contain the low, middle, and
             high 32 bits of the 96-bit integer part of the Decimal. Element 3 contains
             the scale factor and sign of the Decimal: bits 0-15 (the lower word) are
             unused; bits 16-23 contain a value between 0 and 28, indicating the power of
             10 to divide the 96-bit integer part by to produce the Decimal value; bits 24-
             30 are unused; and finally bit 31 indicates the sign of the Decimal value, 0
             meaning positive and 1 meaning negative.

             SQLDECIMAL/SQLNUMERIC has a byte stream of:
             struct {
                 BYTE sign; // 1 if positive, 0 if negative
                 BYTE data[];
             }

             For TDS 7.0 and above, there are always 17 bytes of data
            */

            // write the sign (note that COM and SQL are opposite)
            if (0x80000000 == (stateObj._decimalBits[3] & 0x80000000))
                stateObj.WriteByte(0);
            else
                stateObj.WriteByte(1);

            WriteInt(stateObj._decimalBits[0], stateObj);
            WriteInt(stateObj._decimalBits[1], stateObj);
            WriteInt(stateObj._decimalBits[2], stateObj);
            WriteInt(0, stateObj);
        }

        internal static void WriteIdentifier(string s, TdsParserStateObject stateObj)
        {
            if (null != s)
            {
                stateObj.WriteByte(checked((byte)s.Length));
                WriteString(s, stateObj);
            }
            else
            {
                stateObj.WriteByte((byte)0);
            }
        }

        internal static void WriteIdentifierWithShortLength(string s, TdsParserStateObject stateObj)
        {
            if (null != s)
            {
                WriteShort(checked((short)s.Length), stateObj);
                WriteString(s, stateObj);
            }
            else
            {
                WriteShort(0, stateObj);
            }
        }

        internal static Task WriteString(string s, TdsParserStateObject stateObj, bool canAccumulate = true)
        {
            return WriteString(s, s.Length, 0, stateObj, canAccumulate);
        }

        internal static byte[] SerializeCharArray(char[] carr, int length, int offset)
        {
            int cBytes = ADP.CharSize * length;
            byte[] bytes = new byte[cBytes];

            CopyCharsToBytes(carr, offset, bytes, 0, length);
            return bytes;
        }

        internal static Task WriteCharArray(char[] carr, int length, int offset, TdsParserStateObject stateObj, bool canAccumulate = true)
        {
            int cBytes = ADP.CharSize * length;

            // Perf shortcut: If it fits, write directly to the outBuff
            if (cBytes < (stateObj._outBuff.Length - stateObj._outBytesUsed))
            {
                CopyCharsToBytes(carr, offset, stateObj._outBuff, stateObj._outBytesUsed, length);
                stateObj._outBytesUsed += cBytes;
                return null;
            }
            else
            {
                if (stateObj._bTmp == null || stateObj._bTmp.Length < cBytes)
                {
                    stateObj._bTmp = new byte[cBytes];
                }

                CopyCharsToBytes(carr, offset, stateObj._bTmp, 0, length);
                return stateObj.WriteByteArray(stateObj._bTmp, cBytes, 0, canAccumulate);
            }
        }
    }
}
