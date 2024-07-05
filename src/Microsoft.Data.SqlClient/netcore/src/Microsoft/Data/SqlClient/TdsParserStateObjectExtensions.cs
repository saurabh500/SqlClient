// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Common;
using Microsoft.Data.SqlTypes;

namespace Microsoft.Data.SqlClient
{

    internal partial class TdsParserExtensions
    {
        const int GUID_SIZE = 16;
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

        internal static byte[] SerializeSqlMoney(SqlMoney value, int length, TdsParserStateObject stateObj)
        {
            return TdsParserExtensions.SerializeCurrency(value.Value, length, stateObj);
        }

        internal static byte[] SerializeCurrency(Decimal value, int length, TdsParserStateObject stateObj)
        {
            SqlMoney m = new SqlMoney(value);
            int[] bits = Decimal.GetBits(m.Value);

            // this decimal should be scaled by 10000 (regardless of what the incoming decimal was scaled by)
            bool isNeg = (0 != (bits[3] & unchecked((int)0x80000000)));
            long l = ((long)(uint)bits[1]) << 0x20 | (uint)bits[0];

            if (isNeg)
                l = -l;

            if (length == 4)
            {
                // validate the value can be represented as a small money
                if (value < TdsEnums.SQL_SMALL_MONEY_MIN || value > TdsEnums.SQL_SMALL_MONEY_MAX)
                {
                    throw SQL.MoneyOverflow(value.ToString(CultureInfo.InvariantCulture));
                }

                // We normalize to allow conversion across data types. SMALLMONEY is serialized into a MONEY.
                length = 8;
            }

            Debug.Assert(8 == length, "invalid length in SerializeCurrency");
            if (null == stateObj._bLongBytes)
            {
                stateObj._bLongBytes = new byte[8];
            }

            byte[] bytes = stateObj._bLongBytes;
            int current = 0;

            byte[] bytesPart = TdsParserExtensions.SerializeInt((int)(l >> 0x20), stateObj);
            Buffer.BlockCopy(bytesPart, 0, bytes, current, 4);
            current += 4;

            bytesPart = TdsParserExtensions.SerializeInt((int)l, stateObj);
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

        // For MAX types, this method can only write everything in one big chunk. If multiple
        // chunk writes needed, please use WritePlpBytes/WritePlpChars
        internal static byte[] SerializeUnencryptedValue(object value, MetaType type, byte scale, int actualLength, int offset, bool isDataFeed, byte normalizationVersion, TdsParserStateObject stateObj, Encoding defaultEncoding)
        {
            Debug.Assert((null != value) && (DBNull.Value != value), "unexpected missing or empty object");

            if (normalizationVersion != 0x01)
            {
                throw SQL.UnsupportedNormalizationVersion(normalizationVersion);
            }

            // parameters are always sent over as BIG or N types
            switch (type.NullableType)
            {
                case TdsEnums.SQLFLTN:
                    if (type.FixedLength == 4)
                        return TdsParserExtensions.SerializeFloat((Single)value);
                    else
                    {
                        Debug.Assert(type.FixedLength == 8, "Invalid length for SqlDouble type!");
                        return TdsParserExtensions.SerializeDouble((Double)value);
                    }

                case TdsEnums.SQLBIGBINARY:
                case TdsEnums.SQLBIGVARBINARY:
                case TdsEnums.SQLIMAGE:
                case TdsEnums.SQLUDT:
                    {
                        Debug.Assert(!isDataFeed, "We cannot serialize streams");
                        Debug.Assert(value is byte[], "Value should be an array of bytes");

                        byte[] b = new byte[actualLength];
                        Buffer.BlockCopy((byte[])value, offset, b, 0, actualLength);
                        return b;
                    }

                case TdsEnums.SQLUNIQUEID:
                    {
                        System.Guid guid = (System.Guid)value;
                        byte[] b = guid.ToByteArray();

                        Debug.Assert((actualLength == b.Length) && (actualLength == 16), "Invalid length for guid type in com+ object");
                        return b;
                    }

                case TdsEnums.SQLBITN:
                    {
                        Debug.Assert(type.FixedLength == 1, "Invalid length for SqlBoolean type");

                        // We normalize to allow conversion across data types. BIT is serialized into a BIGINT.
                        return TdsParserExtensions.SerializeLong((bool)value == true ? 1 : 0, stateObj);
                    }

                case TdsEnums.SQLINTN:
                    if (type.FixedLength == 1)
                        return TdsParserExtensions.SerializeLong((byte)value, stateObj);

                    if (type.FixedLength == 2)
                        return TdsParserExtensions.SerializeLong((Int16)value, stateObj);

                    if (type.FixedLength == 4)
                        return TdsParserExtensions.SerializeLong((Int32)value, stateObj);

                    Debug.Assert(type.FixedLength == 8, "invalid length for SqlIntN type:  " + type.FixedLength.ToString(CultureInfo.InvariantCulture));
                    return TdsParserExtensions.SerializeLong((Int64)value, stateObj);

                case TdsEnums.SQLBIGCHAR:
                case TdsEnums.SQLBIGVARCHAR:
                case TdsEnums.SQLTEXT:
                    {
                        Debug.Assert(!isDataFeed, "We cannot serialize streams");
                        Debug.Assert((value is string || value is byte[]), "Value is a byte array or string");

                        if (value is byte[])
                        { // If LazyMat non-filled blob, send cookie rather than value
                            byte[] b = new byte[actualLength];
                            Buffer.BlockCopy((byte[])value, 0, b, 0, actualLength);
                            return b;
                        }
                        else
                        {
                            return TdsParserExtensions.SerializeEncodingChar((string)value, actualLength, offset, defaultEncoding);
                        }
                    }
                case TdsEnums.SQLNCHAR:
                case TdsEnums.SQLNVARCHAR:
                case TdsEnums.SQLNTEXT:
                case TdsEnums.SQLXMLTYPE:
                    {
                        Debug.Assert(!isDataFeed, "We cannot serialize streams");
                        Debug.Assert((value is string || value is byte[]), "Value is a byte array or string");

                        if (value is byte[])
                        { // If LazyMat non-filled blob, send cookie rather than value
                            byte[] b = new byte[actualLength];
                            Buffer.BlockCopy((byte[])value, 0, b, 0, actualLength);
                            return b;
                        }
                        else
                        { // convert to cchars instead of cbytes
                            actualLength >>= 1;
                            return TdsParserExtensions.SerializeString((string)value, actualLength, offset);
                        }
                    }
                case TdsEnums.SQLNUMERICN:
                    Debug.Assert(type.FixedLength <= 17, "Decimal length cannot be greater than 17 bytes");
                    return TdsParserExtensions.SerializeDecimal((Decimal)value, stateObj);

                case TdsEnums.SQLDATETIMN:
                    Debug.Assert(type.FixedLength <= 0xff, "Invalid Fixed Length");

                    TdsDateTime dt = MetaType.FromDateTime((DateTime)value, (byte)type.FixedLength);

                    if (type.FixedLength == 4)
                    {
                        if (0 > dt.days || dt.days > UInt16.MaxValue)
                            throw SQL.SmallDateTimeOverflow(MetaType.ToDateTime(dt.days, dt.time, 4).ToString(CultureInfo.InvariantCulture));

                        if (null == stateObj._bIntBytes)
                        {
                            stateObj._bIntBytes = new byte[4];
                        }

                        byte[] b = stateObj._bIntBytes;
                        int current = 0;

                        byte[] bPart = TdsParserExtensions.SerializeShort(dt.days, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 2);
                        current += 2;

                        bPart = TdsParserExtensions.SerializeShort(dt.time, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 2);

                        return b;
                    }
                    else
                    {
                        if (null == stateObj._bLongBytes)
                        {
                            stateObj._bLongBytes = new byte[8];
                        }
                        byte[] b = stateObj._bLongBytes;
                        int current = 0;

                        byte[] bPart = TdsParserExtensions.SerializeInt(dt.days, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 4);
                        current += 4;

                        bPart = TdsParserExtensions.SerializeInt(dt.time, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 4);

                        return b;
                    }

                case TdsEnums.SQLMONEYN:
                    {
                        return TdsParserExtensions.SerializeCurrency((Decimal)value, type.FixedLength, stateObj);
                    }

                case TdsEnums.SQLDATE:
                    {
                        return TdsParserExtensions.SerializeDate((DateTime)value);
                    }

                case TdsEnums.SQLTIME:
                    if (scale > TdsEnums.DEFAULT_VARTIME_SCALE)
                    {
                        throw SQL.TimeScaleValueOutOfRange(scale);
                    }
                    return TdsParserExtensions.SerializeTime((TimeSpan)value, scale, actualLength);

                case TdsEnums.SQLDATETIME2:
                    if (scale > TdsEnums.DEFAULT_VARTIME_SCALE)
                    {
                        throw SQL.TimeScaleValueOutOfRange(scale);
                    }
                    return TdsParserExtensions.SerializeDateTime2((DateTime)value, scale, actualLength);

                case TdsEnums.SQLDATETIMEOFFSET:
                    if (scale > TdsEnums.DEFAULT_VARTIME_SCALE)
                    {
                        throw SQL.TimeScaleValueOutOfRange(scale);
                    }
                    return TdsParserExtensions.SerializeDateTimeOffset((DateTimeOffset)value, scale, actualLength);

                default:
                    throw SQL.UnsupportedDatatypeEncryption(type.TypeName);
            } // switch
        }

        // For MAX types, this method can only write everything in one big chunk. If multiple
        // chunk writes needed, please use WritePlpBytes/WritePlpChars
        internal static byte[] SerializeUnencryptedSqlValue(object value, MetaType type, int actualLength, int offset, byte normalizationVersion, TdsParserStateObject stateObj, Encoding defaultEncoding)
        {
            Debug.Assert(((type.NullableType == TdsEnums.SQLXMLTYPE) ||
                   (value is INullable && !((INullable)value).IsNull)),
                   "unexpected null SqlType!");

            if (normalizationVersion != 0x01)
            {
                throw SQL.UnsupportedNormalizationVersion(normalizationVersion);
            }

            // parameters are always sent over as BIG or N types
            switch (type.NullableType)
            {
                case TdsEnums.SQLFLTN:
                    if (type.FixedLength == 4)
                        return TdsParserExtensions.SerializeFloat(((SqlSingle)value).Value);
                    else
                    {
                        Debug.Assert(type.FixedLength == 8, "Invalid length for SqlDouble type!");
                        return TdsParserExtensions.SerializeDouble(((SqlDouble)value).Value);
                    }

                case TdsEnums.SQLBIGBINARY:
                case TdsEnums.SQLBIGVARBINARY:
                case TdsEnums.SQLIMAGE:
                    {
                        byte[] b = new byte[actualLength];

                        if (value is SqlBinary)
                        {
                            Buffer.BlockCopy(((SqlBinary)value).Value, offset, b, 0, actualLength);
                        }
                        else
                        {
                            Debug.Assert(value is SqlBytes);
                            Buffer.BlockCopy(((SqlBytes)value).Value, offset, b, 0, actualLength);
                        }
                        return b;
                    }

                case TdsEnums.SQLUNIQUEID:
                    {
                        byte[] b = ((SqlGuid)value).ToByteArray();

                        Debug.Assert((actualLength == b.Length) && (actualLength == 16), "Invalid length for guid type in com+ object");
                        return b;
                    }

                case TdsEnums.SQLBITN:
                    {
                        Debug.Assert(type.FixedLength == 1, "Invalid length for SqlBoolean type");

                        // We normalize to allow conversion across data types. BIT is serialized into a BIGINT.
                        return TdsParserExtensions.SerializeLong(((SqlBoolean)value).Value == true ? 1 : 0, stateObj);
                    }

                case TdsEnums.SQLINTN:
                    // We normalize to allow conversion across data types. All data types below are serialized into a BIGINT.
                    if (type.FixedLength == 1)
                        return TdsParserExtensions.SerializeLong(((SqlByte)value).Value, stateObj);

                    if (type.FixedLength == 2)
                        return TdsParserExtensions.SerializeLong(((SqlInt16)value).Value, stateObj);

                    if (type.FixedLength == 4)
                        return TdsParserExtensions.SerializeLong(((SqlInt32)value).Value, stateObj);
                    else
                    {
                        Debug.Assert(type.FixedLength == 8, "invalid length for SqlIntN type:  " + type.FixedLength.ToString(CultureInfo.InvariantCulture));
                        return TdsParserExtensions.SerializeLong(((SqlInt64)value).Value, stateObj);
                    }

                case TdsEnums.SQLBIGCHAR:
                case TdsEnums.SQLBIGVARCHAR:
                case TdsEnums.SQLTEXT:
                    if (value is SqlChars)
                    {
                        String sch = new String(((SqlChars)value).Value);
                        return TdsParserExtensions.SerializeEncodingChar(sch, actualLength, offset, defaultEncoding);
                    }
                    else
                    {
                        Debug.Assert(value is SqlString);
                        return TdsParserExtensions.SerializeEncodingChar(((SqlString)value).Value, actualLength, offset, defaultEncoding);
                    }


                case TdsEnums.SQLNCHAR:
                case TdsEnums.SQLNVARCHAR:
                case TdsEnums.SQLNTEXT:
                case TdsEnums.SQLXMLTYPE:
                    // convert to cchars instead of cbytes
                    // Xml type is already converted to string through GetCoercedValue
                    if (actualLength != 0)
                        actualLength >>= 1;

                    if (value is SqlChars)
                    {
                        return TdsParserExtensions.SerializeCharArray(((SqlChars)value).Value, actualLength, offset);
                    }
                    else
                    {
                        Debug.Assert(value is SqlString);
                        return TdsParserExtensions.SerializeString(((SqlString)value).Value, actualLength, offset);
                    }

                case TdsEnums.SQLNUMERICN:
                    Debug.Assert(type.FixedLength <= 17, "Decimal length cannot be greater than 17 bytes");
                    return TdsParserExtensions.SerializeSqlDecimal((SqlDecimal)value, stateObj);

                case TdsEnums.SQLDATETIMN:
                    SqlDateTime dt = (SqlDateTime)value;

                    if (type.FixedLength == 4)
                    {
                        if (0 > dt.DayTicks || dt.DayTicks > UInt16.MaxValue)
                            throw SQL.SmallDateTimeOverflow(dt.ToString());

                        if (null == stateObj._bIntBytes)
                        {
                            stateObj._bIntBytes = new byte[4];
                        }

                        byte[] b = stateObj._bIntBytes;
                        int current = 0;

                        byte[] bPart = TdsParserExtensions.SerializeShort(dt.DayTicks, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 2);
                        current += 2;

                        bPart = TdsParserExtensions.SerializeShort(dt.TimeTicks / SqlDateTime.SQLTicksPerMinute, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 2);

                        return b;
                    }
                    else
                    {
                        if (null == stateObj._bLongBytes)
                        {
                            stateObj._bLongBytes = new byte[8];
                        }

                        byte[] b = stateObj._bLongBytes;
                        int current = 0;

                        byte[] bPart = TdsParserExtensions.SerializeInt(dt.DayTicks, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 4);
                        current += 4;

                        bPart = TdsParserExtensions.SerializeInt(dt.TimeTicks, stateObj);
                        Buffer.BlockCopy(bPart, 0, b, current, 4);

                        return b;
                    }

                case TdsEnums.SQLMONEYN:
                    {
                        return TdsParserExtensions.SerializeSqlMoney((SqlMoney)value, type.FixedLength, stateObj);
                    }

                default:
                    throw SQL.UnsupportedDatatypeEncryption(type.TypeName);
            } // switch
        }


        /// <summary>
        /// Encrypts a column value (for SqlBulkCopy)
        /// </summary>
        /// <returns></returns>
        internal static object EncryptColumnValue(object value, SqlMetaDataPriv metadata, string column, TdsParserStateObject stateObj, bool isDataFeed, bool isSqlType, TdsParser parser)
        {
            Debug.Assert(parser.IsColumnEncryptionSupported, "Server doesn't support encryption, yet we received encryption metadata");
            Debug.Assert(parser.ShouldEncryptValuesForBulkCopy(), "Encryption attempted when not requested");

            if (isDataFeed)
            { // can't encrypt a stream column
                SQL.StreamNotSupportOnEncryptedColumn(column);
            }

            int actualLengthInBytes;
            switch (metadata.baseTI.metaType.NullableType)
            {
                case TdsEnums.SQLBIGBINARY:
                case TdsEnums.SQLBIGVARBINARY:
                case TdsEnums.SQLIMAGE:
                    // For some datatypes, engine does truncation before storing the value. (For example, when
                    // trying to insert a varbinary(7000) into a varbinary(3000) column). Since we encrypt the
                    // column values, engine has no way to tell the size of the plaintext datatype. Therefore,
                    // we truncate the values based on target column sizes here before encrypting them. This
                    // truncation is only needed if we exceed the max column length or if the target column is
                    // not a blob type (eg. varbinary(max)). The actual work of truncating the column happens
                    // when we normalize and serialize the data buffers. The serialization routine expects us
                    // to report the size of data to be copied out (for serialization). If we underreport the
                    // size, truncation will happen for us!
                    actualLengthInBytes = (isSqlType) ? ((SqlBinary)value).Length : ((byte[])value).Length;
                    if (metadata.baseTI.length > 0 &&
                        actualLengthInBytes > metadata.baseTI.length)
                    {
                        // see comments above
                        actualLengthInBytes = metadata.baseTI.length;
                    }
                    break;

                case TdsEnums.SQLUNIQUEID:
                    actualLengthInBytes = GUID_SIZE;   // that's a constant for guid
                    break;
                case TdsEnums.SQLBIGCHAR:
                case TdsEnums.SQLBIGVARCHAR:
                case TdsEnums.SQLTEXT:
                    if (null == parser._defaultEncoding)
                    {
                        parser.ThrowUnsupportedCollationEncountered(null); // stateObject only when reading
                    }

                    string stringValue = (isSqlType) ? ((SqlString)value).Value : (string)value;
                    actualLengthInBytes = parser._defaultEncoding.GetByteCount(stringValue);

                    // If the string length is > max length, then use the max length (see comments above)
                    if (metadata.baseTI.length > 0 &&
                        actualLengthInBytes > metadata.baseTI.length)
                    {
                        actualLengthInBytes = metadata.baseTI.length; // this ensure truncation!
                    }

                    break;
                case TdsEnums.SQLNCHAR:
                case TdsEnums.SQLNVARCHAR:
                case TdsEnums.SQLNTEXT:
                    actualLengthInBytes = ((isSqlType) ? ((SqlString)value).Value.Length : ((string)value).Length) * 2;

                    if (metadata.baseTI.length > 0 &&
                        actualLengthInBytes > metadata.baseTI.length)
                    { // see comments above
                        actualLengthInBytes = metadata.baseTI.length;
                    }

                    break;

                default:
                    actualLengthInBytes = metadata.baseTI.length;
                    break;
            }

            byte[] serializedValue;
            if (isSqlType)
            {
                // SqlType
                serializedValue = TdsParserExtensions.SerializeUnencryptedSqlValue(value,
                                            metadata.baseTI.metaType,
                                            actualLengthInBytes,
                                            offset: 0,
                                            normalizationVersion: metadata.cipherMD.NormalizationRuleVersion,
                                            stateObj: stateObj,
                                            defaultEncoding: parser._defaultEncoding);
            }
            else
            {
                serializedValue = TdsParserExtensions.SerializeUnencryptedValue(value,
                                            metadata.baseTI.metaType,
                                            metadata.baseTI.scale,
                                            actualLengthInBytes,
                                            offset: 0,
                                            isDataFeed: isDataFeed,
                                            normalizationVersion: metadata.cipherMD.NormalizationRuleVersion,
                                            stateObj: stateObj,
                                            defaultEncoding: parser._defaultEncoding);
            }

            Debug.Assert(serializedValue != null, "serializedValue should not be null in TdsExecuteRPC.");
            return SqlSecurityUtility.EncryptWithKey(
                    serializedValue,
                    metadata.cipherMD,
                    parser.Connection.Connection,
                    null);
        }


        //
        // returns the token length of the token or tds type
        // Returns -1 for partially length prefixed (plp) types for metadata info.
        // DOES NOT handle plp data streams correctly!!!
        // Plp data streams length information should be obtained from GetDataLength
        //
        internal static bool TryGetTokenLength(byte token, TdsParserStateObject stateObj, out int tokenLength)
        {
            Debug.Assert(token != 0, "0 length token!");

            switch (token)
            { // rules about SQLLenMask no longer apply to new tokens (as of 7.4)
                case TdsEnums.SQLFEATUREEXTACK:
                    tokenLength = -1;
                    return true;
                case TdsEnums.SQLSESSIONSTATE:
                    return stateObj.TryReadInt32(out tokenLength);
                case TdsEnums.SQLFEDAUTHINFO:
                    return stateObj.TryReadInt32(out tokenLength);
            }

            {
                if (token == TdsEnums.SQLUDT)
                { // special case for UDTs
                    tokenLength = -1; // Should we return -1 or not call GetTokenLength for UDTs?
                    return true;
                }
                else if (token == TdsEnums.SQLRETURNVALUE)
                {
                    tokenLength = -1; // In 2005, the RETURNVALUE token stream no longer has length
                    return true;
                }
                else if (token == TdsEnums.SQLXMLTYPE)
                {
                    ushort value;
                    if (!stateObj.TryReadUInt16(out value))
                    {
                        tokenLength = 0;
                        return false;
                    }
                    tokenLength = (int)value;
                    Debug.Assert(tokenLength == TdsEnums.SQL_USHORTVARMAXLEN, "Invalid token stream for xml datatype");
                    return true;
                }
            }

            switch (token & TdsEnums.SQLLenMask)
            {
                case TdsEnums.SQLFixedLen:
                    tokenLength = ((0x01 << ((token & 0x0c) >> 2))) & 0xff;
                    return true;
                case TdsEnums.SQLZeroLen:
                    tokenLength = 0;
                    return true;
                case TdsEnums.SQLVarLen:
                case TdsEnums.SQLVarCnt:
                    if (0 != (token & 0x80))
                    {
                        ushort value;
                        if (!stateObj.TryReadUInt16(out value))
                        {
                            tokenLength = 0;
                            return false;
                        }
                        tokenLength = value;
                        return true;
                    }
                    else if (0 == (token & 0x0c))
                    {
                        if (!stateObj.TryReadInt32(out tokenLength))
                        {
                            return false;
                        }
                        return true;
                    }
                    else
                    {
                        byte value;
                        if (!stateObj.TryReadByte(out value))
                        {
                            tokenLength = 0;
                            return false;
                        }
                        tokenLength = value;
                        return true;
                    }
                default:
                    Debug.Fail("Unknown token length!");
                    tokenLength = 0;
                    return true;
            }
        }

        /// <summary>
        /// Checks if the given token is a valid TDS token
        /// </summary>
        /// <param name="token">Token to check</param>
        /// <returns>True if the token is a valid TDS token, otherwise false</returns>
        internal static bool IsValidTdsToken(byte token)
        {
            return (
                token == TdsEnums.SQLERROR ||
                token == TdsEnums.SQLINFO ||
                token == TdsEnums.SQLLOGINACK ||
                token == TdsEnums.SQLENVCHANGE ||
                token == TdsEnums.SQLRETURNVALUE ||
                token == TdsEnums.SQLRETURNSTATUS ||
                token == TdsEnums.SQLCOLNAME ||
                token == TdsEnums.SQLCOLFMT ||
                token == TdsEnums.SQLRESCOLSRCS ||
                token == TdsEnums.SQLDATACLASSIFICATION ||
                token == TdsEnums.SQLCOLMETADATA ||
                token == TdsEnums.SQLALTMETADATA ||
                token == TdsEnums.SQLTABNAME ||
                token == TdsEnums.SQLCOLINFO ||
                token == TdsEnums.SQLORDER ||
                token == TdsEnums.SQLALTROW ||
                token == TdsEnums.SQLROW ||
                token == TdsEnums.SQLNBCROW ||
                token == TdsEnums.SQLDONE ||
                token == TdsEnums.SQLDONEPROC ||
                token == TdsEnums.SQLDONEINPROC ||
                token == TdsEnums.SQLROWCRC ||
                token == TdsEnums.SQLSECLEVEL ||
                token == TdsEnums.SQLPROCID ||
                token == TdsEnums.SQLOFFSET ||
                token == TdsEnums.SQLSSPI ||
                token == TdsEnums.SQLFEATUREEXTACK ||
                token == TdsEnums.SQLSESSIONSTATE ||
                token == TdsEnums.SQLFEDAUTHINFO);
        }

        internal static bool TryProcessDone(SqlCommand cmd, SqlDataReader reader, ref RunBehavior run, TdsParserStateObject stateObj, string server, SqlStatistics _statistics, bool _statisticsIsInTransaction)
        {
            ushort curCmd;
            ushort status;
            int count;

            if (LocalAppContextSwitches.MakeReadAsyncBlocking)
            {
                // Don't retry TryProcessDone
                stateObj._syncOverAsync = true;
            }

            // status
            // command
            // rowcount (valid only if DONE_COUNT bit is set)

            if (!stateObj.TryReadUInt16(out status))
            {
                return false;
            }
            if (!stateObj.TryReadUInt16(out curCmd))
            {
                return false;
            }

            long longCount;
            if (!stateObj.TryReadInt64(out longCount))
            {
                return false;
            }
            count = (int)longCount;

            // We get a done token with the attention bit set
            if (TdsEnums.DONE_ATTN == (status & TdsEnums.DONE_ATTN))
            {
                Debug.Assert(TdsEnums.DONE_MORE != (status & TdsEnums.DONE_MORE), "Not expecting DONE_MORE when receiving DONE_ATTN");
                Debug.Assert(stateObj._attentionSent, "Received attention done without sending one!");
                stateObj.HasReceivedAttention = true;
                Debug.Assert(stateObj._inBytesUsed == stateObj._inBytesRead && stateObj._inBytesPacket == 0, "DONE_ATTN received with more data left on wire");
            }
            if ((null != cmd) && (TdsEnums.DONE_COUNT == (status & TdsEnums.DONE_COUNT)))
            {
                if (curCmd != TdsEnums.SELECT)
                {
                    if (cmd.IsDescribeParameterEncryptionRPCCurrentlyInProgress)
                    {
                        // The below line is used only for debug asserts and not exposed publicly or impacts functionality otherwise.
                        cmd.RowsAffectedByDescribeParameterEncryption = count;
                    }
                    else
                    {
                        cmd.InternalRecordsAffected = count;
                    }
                }
                // Skip the bogus DONE counts sent by the server
                if (stateObj.HasReceivedColumnMetadata || (curCmd != TdsEnums.SELECT))
                {
                    cmd.OnStatementCompleted(count);
                }
            }

            stateObj.HasReceivedColumnMetadata = false;

            // Surface exception for DONE_ERROR in the case we did not receive an error token
            // in the stream, but an error occurred.  In these cases, we throw a general server error.  The
            // situations where this can occur are: an invalid buffer received from client, login error
            // and the server refused our connection, and the case where we are trying to log in but
            // the server has reached its max connection limit.  Bottom line, we need to throw general
            // error in the cases where we did not receive an error token along with the DONE_ERROR.
            if ((TdsEnums.DONE_ERROR == (TdsEnums.DONE_ERROR & status)) && stateObj.ErrorCount == 0 &&
                  stateObj.HasReceivedError == false && (RunBehavior.Clean != (RunBehavior.Clean & run)))
            {
                stateObj.AddError(new SqlError(0, 0, TdsEnums.MIN_ERROR_CLASS, server, SQLMessage.SevereError(), "", 0, exception: null, batchIndex: cmd?.GetCurrentBatchIndex() ?? -1));

                if (null != reader)
                {
                    if (!reader.IsInitialized)
                    {
                        run = RunBehavior.UntilDone;
                    }
                }
            }

            // Similar to above, only with a more severe error.  In this case, if we received
            // the done_srverror, this exception will be added to the collection regardless.
            // The server will always break the connection in this case.
            if ((TdsEnums.DONE_SRVERROR == (TdsEnums.DONE_SRVERROR & status)) && (RunBehavior.Clean != (RunBehavior.Clean & run)))
            {
                stateObj.AddError(new SqlError(0, 0, TdsEnums.FATAL_ERROR_CLASS, server, SQLMessage.SevereError(), "", 0, exception: null, batchIndex: cmd?.GetCurrentBatchIndex() ?? -1));

                if (null != reader)
                {
                    if (!reader.IsInitialized)
                    {
                        run = RunBehavior.UntilDone;
                    }
                }
            }

            ProcessSqlStatistics(curCmd, status, count, _statistics, _statisticsIsInTransaction);

            // stop if the DONE_MORE bit isn't set (see above for attention handling)
            if (TdsEnums.DONE_MORE != (status & TdsEnums.DONE_MORE))
            {
                stateObj.HasReceivedError = false;
                if (stateObj._inBytesUsed >= stateObj._inBytesRead)
                {
                    stateObj.HasPendingData = false;
                }
            }

            // _pendingData set by e.g. 'TdsExecuteSQLBatch'
            // _hasOpenResult always set to true by 'WriteMarsHeader'
            //
            if (!stateObj.HasPendingData && stateObj.HasOpenResult)
            {
                /*
                                Debug.Assert(!((sqlTransaction != null               && _distributedTransaction != null) ||
                                                (_userStartedLocalTransaction != null && _distributedTransaction != null))
                                                , "ProcessDone - have both distributed and local transactions not null!");
                */
                // WebData 112722

                stateObj.DecrementOpenResultCount();
            }

            return true;
        }

        private static void ProcessSqlStatistics(ushort curCmd, ushort status, int count, SqlStatistics _statistics, bool statisticsIsInTransaction)
        {
            // SqlStatistics bookkeeping stuff
            //
            if (null != _statistics)
            {
                // any done after row(s) counts as a resultset
                if (_statistics.WaitForDoneAfterRow)
                {
                    _statistics.SafeIncrement(ref _statistics._sumResultSets);
                    _statistics.WaitForDoneAfterRow = false;
                }

                // clear row count DONE_COUNT flag is not set
                if (!(TdsEnums.DONE_COUNT == (status & TdsEnums.DONE_COUNT)))
                {
                    count = 0;
                }

                switch (curCmd)
                {
                    case TdsEnums.INSERT:
                    case TdsEnums.DELETE:
                    case TdsEnums.UPDATE:
                    case TdsEnums.MERGE:
                        _statistics.SafeIncrement(ref _statistics._iduCount);
                        _statistics.SafeAdd(ref _statistics._iduRows, count);
                        if (!statisticsIsInTransaction)
                        {
                            _statistics.SafeIncrement(ref _statistics._transactions);
                        }

                        break;

                    case TdsEnums.SELECT:
                        _statistics.SafeIncrement(ref _statistics._selectCount);
                        _statistics.SafeAdd(ref _statistics._selectRows, count);
                        break;

                    case TdsEnums.BEGINXACT:
                        if (!statisticsIsInTransaction)
                        {
                            _statistics.SafeIncrement(ref _statistics._transactions);
                        }
                        statisticsIsInTransaction = true;
                        break;

                    case TdsEnums.OPENCURSOR:
                        _statistics.SafeIncrement(ref _statistics._cursorOpens);
                        break;

                    case TdsEnums.ABORT:
                        statisticsIsInTransaction = false;
                        break;

                    case TdsEnums.ENDXACT:
                        statisticsIsInTransaction = false;
                        break;
                } // switch
            }
            else
            {
                switch (curCmd)
                {
                    case TdsEnums.BEGINXACT:
                        statisticsIsInTransaction = true;
                        break;

                    case TdsEnums.ABORT:
                    case TdsEnums.ENDXACT:
                        statisticsIsInTransaction = false;
                        break;
                }
            }
        }

        internal static bool TryProcessFeatureExtAck(TdsParserStateObject stateObj, 
            SqlInternalConnectionTds _connHandler, 
            string FQDNforDNSCache, 
            bool IsColumnEncryptionSupported, 
            int TceVersionSupported, 
            string EnclaveType)
        {
            // read feature ID
            byte featureId;
            do
            {
                if (!stateObj.TryReadByte(out featureId))
                {
                    return false;
                }
                if (featureId != TdsEnums.FEATUREEXT_TERMINATOR)
                {
                    uint dataLen;
                    if (!stateObj.TryReadUInt32(out dataLen))
                    {
                        return false;
                    }
                    byte[] data = new byte[dataLen];
                    if (dataLen > 0)
                    {
                        if (!stateObj.TryReadByteArray(data, checked((int)dataLen)))
                        {
                            return false;
                        }
                    }
                    _connHandler.OnFeatureExtAck(featureId, data);
                }
            } while (featureId != TdsEnums.FEATUREEXT_TERMINATOR);

            // Write to DNS Cache or clean up DNS Cache for TCP protocol
            bool ret = false;
            if (_connHandler._cleanSQLDNSCaching)
            {
                ret = SQLFallbackDNSCache.Instance.DeleteDNSInfo(FQDNforDNSCache);
            }

            if (_connHandler.IsSQLDNSCachingSupported && _connHandler.pendingSQLDNSObject != null
                    && !SQLFallbackDNSCache.Instance.IsDuplicate(_connHandler.pendingSQLDNSObject))
            {
                ret = SQLFallbackDNSCache.Instance.AddDNSInfo(_connHandler.pendingSQLDNSObject);
                _connHandler.pendingSQLDNSObject = null;
            }

            // Check if column encryption was on and feature wasn't acknowledged and we aren't going to be routed to another server.
            if (_connHandler.RoutingInfo == null
                && _connHandler.ConnectionOptions.ColumnEncryptionSetting == SqlConnectionColumnEncryptionSetting.Enabled
                && !IsColumnEncryptionSupported)
            {
                throw SQL.TceNotSupported();
            }

            // Check if server does not support Enclave Computations and we aren't going to be routed to another server.
            if (_connHandler.RoutingInfo == null)
            {
                SqlConnectionAttestationProtocol attestationProtocol = _connHandler.ConnectionOptions.AttestationProtocol;

                if (TceVersionSupported < TdsEnums.MIN_TCE_VERSION_WITH_ENCLAVE_SUPPORT)
                {
                    // Check if enclave attestation url was specified and server does not support enclave computations and we aren't going to be routed to another server.
                    if (!string.IsNullOrWhiteSpace(_connHandler.ConnectionOptions.EnclaveAttestationUrl) && attestationProtocol != SqlConnectionAttestationProtocol.NotSpecified)
                    {
                        throw SQL.EnclaveComputationsNotSupported();
                    }
                    else if (!string.IsNullOrWhiteSpace(_connHandler.ConnectionOptions.EnclaveAttestationUrl))
                    {
                        throw SQL.AttestationURLNotSupported();
                    }
                    else if (_connHandler.ConnectionOptions.AttestationProtocol != SqlConnectionAttestationProtocol.NotSpecified)
                    {
                        throw SQL.AttestationProtocolNotSupported();
                    }
                }

                // Check if enclave attestation url was specified and server does not return an enclave type and we aren't going to be routed to another server.
                if (!string.IsNullOrWhiteSpace(_connHandler.ConnectionOptions.EnclaveAttestationUrl) || attestationProtocol == SqlConnectionAttestationProtocol.None)
                {
                    if (string.IsNullOrWhiteSpace(EnclaveType))
                    {
                        throw SQL.EnclaveTypeNotReturned();
                    }
                    else
                    {
                        // Check if the attestation protocol is specified and supports the enclave type.
                        if (SqlConnectionAttestationProtocol.NotSpecified != attestationProtocol && !IsValidAttestationProtocol(attestationProtocol, EnclaveType))
                        {
                            throw SQL.AttestationProtocolNotSupportEnclaveType(attestationProtocol.ToString(), EnclaveType);
                        }
                    }
                }
            }

            return true;
        }

        private static bool IsValidAttestationProtocol(SqlConnectionAttestationProtocol attestationProtocol, string enclaveType)
        {
            switch (enclaveType.ToUpper())
            {
                case TdsEnums.ENCLAVE_TYPE_VBS:
                    if (attestationProtocol != SqlConnectionAttestationProtocol.AAS
                        && attestationProtocol != SqlConnectionAttestationProtocol.HGS
                        && attestationProtocol != SqlConnectionAttestationProtocol.None)
                    {
                        return false;
                    }
                    break;

                case TdsEnums.ENCLAVE_TYPE_SGX:
#if ENCLAVE_SIMULATOR
                    if (attestationProtocol != SqlConnectionAttestationProtocol.AAS
                        && attestationProtocol != SqlConnectionAttestationProtocol.None)
#else
                    if (attestationProtocol != SqlConnectionAttestationProtocol.AAS)
#endif
                    {
                        return false;
                    }
                    break;

#if ENCLAVE_SIMULATOR
                case TdsEnums.ENCLAVE_TYPE_SIMULATOR:
                    if (attestationProtocol != SqlConnectionAttestationProtocol.None)
                    {
                        return false;
                    }
                    break;
#endif
                default:
                    // if we reach here, the enclave type is not supported
                    throw SQL.EnclaveTypeNotSupported(enclaveType);
            }

            return true;
        }


        internal static bool TryProcessSessionState(TdsParserStateObject stateObj, int length, SqlInternalConnectionTds _connHandler)
        {
            SessionData sdata = _connHandler._currentSessionData;
            if (length < 5)
            {
                throw SQL.ParsingError();
            }
            uint seqNum;
            if (!stateObj.TryReadUInt32(out seqNum))
            {
                return false;
            }
            if (seqNum == uint.MaxValue)
            {
                _connHandler.DoNotPoolThisConnection();
            }
            byte status;
            if (!stateObj.TryReadByte(out status))
            {
                return false;
            }
            if (status > 1)
            {
                throw SQL.ParsingError();
            }
            bool recoverable = status != 0;
            length -= 5;
            while (length > 0)
            {
                byte stateId;
                if (!stateObj.TryReadByte(out stateId))
                {
                    return false;
                }
                int stateLen;
                byte stateLenByte;
                if (!stateObj.TryReadByte(out stateLenByte))
                {
                    return false;
                }
                if (stateLenByte < 0xFF)
                {
                    stateLen = stateLenByte;
                }
                else
                {
                    if (!stateObj.TryReadInt32(out stateLen))
                    {
                        return false;
                    }
                }
                byte[] buffer = null;
                lock (sdata._delta)
                {
                    if (sdata._delta[stateId] == null)
                    {
                        buffer = new byte[stateLen];
                        sdata._delta[stateId] = new SessionStateRecord { _version = seqNum, _dataLength = stateLen, _data = buffer, _recoverable = recoverable };
                        sdata._deltaDirty = true;
                        if (!recoverable)
                        {
                            checked
                            {
                                sdata._unrecoverableStatesCount++;
                            }
                        }
                    }
                    else
                    {
                        if (sdata._delta[stateId]._version <= seqNum)
                        {
                            SessionStateRecord sv = sdata._delta[stateId];
                            sv._version = seqNum;
                            sv._dataLength = stateLen;
                            if (sv._recoverable != recoverable)
                            {
                                if (recoverable)
                                {
                                    Debug.Assert(sdata._unrecoverableStatesCount > 0, "Unrecoverable states count >0");
                                    sdata._unrecoverableStatesCount--;
                                }
                                else
                                {
                                    checked
                                    {
                                        sdata._unrecoverableStatesCount++;
                                    }
                                }
                                sv._recoverable = recoverable;
                            }
                            buffer = sv._data;
                            if (buffer.Length < stateLen)
                            {
                                buffer = new byte[stateLen];
                                sv._data = buffer;
                            }
                        }
                    }
                }
                if (buffer != null)
                {
                    if (!stateObj.TryReadByteArray(buffer, stateLen))
                    {
                        return false;
                    }
                }
                else
                {
                    if (!stateObj.TrySkipBytes(stateLen))
                        return false;
                }

                if (stateLenByte < 0xFF)
                {
                    length -= 2 + stateLen;
                }
                else
                {
                    length -= 6 + stateLen;
                }
            }
            sdata.AssertUnrecoverableStateCountIsCorrect();

            return true;
        }

        internal static bool TryProcessFedAuthInfo(TdsParserStateObject stateObj, int tokenLen, out SqlFedAuthInfo sqlFedAuthInfo)
        {
            sqlFedAuthInfo = null;
            SqlFedAuthInfo tempFedAuthInfo = new SqlFedAuthInfo();

            // Skip reading token length, since it has already been read in caller
            SqlClientEventSource.Log.TryAdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo> FEDAUTHINFO token stream length = {0}", tokenLen);
            if (tokenLen < sizeof(uint))
            {
                // the token must at least contain a DWORD indicating the number of info IDs
                SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> FEDAUTHINFO token stream length too short for CountOfInfoIDs.");
                throw SQL.ParsingErrorLength(ParsingErrorState.FedAuthInfoLengthTooShortForCountOfInfoIds, tokenLen);
            }

            // read how many FedAuthInfo options there are
            uint optionsCount;
            if (!stateObj.TryReadUInt32(out optionsCount))
            {
                SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> Failed to read CountOfInfoIDs in FEDAUTHINFO token stream.");
                throw SQL.ParsingError(ParsingErrorState.FedAuthInfoFailedToReadCountOfInfoIds);
            }
            tokenLen -= sizeof(uint); // remaining length is shortened since we read optCount
            if (SqlClientEventSource.Log.IsAdvancedTraceOn())
            {
                SqlClientEventSource.Log.AdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ADV> CountOfInfoIDs = {0}", optionsCount.ToString(CultureInfo.InvariantCulture));
            }
            if (tokenLen > 0)
            {
                // read the rest of the token
                byte[] tokenData = new byte[tokenLen];
                int totalRead = 0;
                bool successfulRead = stateObj.TryReadByteArray(tokenData, tokenLen, out totalRead);
                if (SqlClientEventSource.Log.IsAdvancedTraceOn())
                {
                    SqlClientEventSource.Log.AdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ADV> Read rest of FEDAUTHINFO token stream: {0}", BitConverter.ToString(tokenData, 0, totalRead));
                }
                if (!successfulRead || totalRead != tokenLen)
                {
                    SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> Failed to read FEDAUTHINFO token stream. Attempted to read {0} bytes, actually read {1}", tokenLen, totalRead);
                    throw SQL.ParsingError(ParsingErrorState.FedAuthInfoFailedToReadTokenStream);
                }

                // each FedAuthInfoOpt is 9 bytes:
                //    1 byte for FedAuthInfoID
                //    4 bytes for FedAuthInfoDataLen
                //    4 bytes for FedAuthInfoDataOffset
                // So this is the index in tokenData for the i-th option
                const uint optionSize = 9;

                // the total number of bytes for all FedAuthInfoOpts together
                uint totalOptionsSize = checked(optionsCount * optionSize);

                for (uint i = 0; i < optionsCount; i++)
                {
                    uint currentOptionOffset = checked(i * optionSize);

                    byte id = tokenData[currentOptionOffset];
                    uint dataLen = BinaryPrimitives.ReadUInt32LittleEndian(tokenData.AsSpan(checked((int)(currentOptionOffset + 1))));
                    uint dataOffset = BinaryPrimitives.ReadUInt32LittleEndian(tokenData.AsSpan(checked((int)(currentOptionOffset + 5))));
                    if (SqlClientEventSource.Log.IsAdvancedTraceOn())
                    {
                        SqlClientEventSource.Log.AdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo> FedAuthInfoOpt: ID={0}, DataLen={1}, Offset={2}", id, dataLen.ToString(CultureInfo.InvariantCulture), dataOffset.ToString(CultureInfo.InvariantCulture));
                    }

                    // offset is measured from optCount, so subtract to make offset measured
                    // from the beginning of tokenData
                    checked
                    {
                        dataOffset -= sizeof(uint);
                    }

                    // if dataOffset points to a region within FedAuthInfoOpt or after the end of the token, throw
                    if (dataOffset < totalOptionsSize || dataOffset >= tokenLen)
                    {
                        SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> FedAuthInfoDataOffset points to an invalid location.");
                        throw SQL.ParsingErrorOffset(ParsingErrorState.FedAuthInfoInvalidOffset, unchecked((int)dataOffset));
                    }

                    // try to read data and throw if the arguments are bad, meaning the server sent us a bad token
                    string data;
                    try
                    {
                        data = System.Text.Encoding.Unicode.GetString(tokenData, checked((int)dataOffset), checked((int)dataLen));
                    }
                    catch (ArgumentOutOfRangeException e)
                    {
                        SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> Failed to read FedAuthInfoData.");
                        throw SQL.ParsingError(ParsingErrorState.FedAuthInfoFailedToReadData, e);
                    }
                    catch (ArgumentException e)
                    {
                        SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> FedAuthInfoData is not in unicode format.");
                        throw SQL.ParsingError(ParsingErrorState.FedAuthInfoDataNotUnicode, e);
                    }
                    SqlClientEventSource.Log.TryAdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ADV> FedAuthInfoData: {0}", data);

                    // store data in tempFedAuthInfo
                    switch ((TdsEnums.FedAuthInfoId)id)
                    {
                        case TdsEnums.FedAuthInfoId.Spn:
                            tempFedAuthInfo.spn = data;
                            break;

                        case TdsEnums.FedAuthInfoId.Stsurl:
                            tempFedAuthInfo.stsurl = data;
                            break;

                        default:
                            SqlClientEventSource.Log.TryAdvancedTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ADV> Ignoring unknown federated authentication info option: {0}", id);
                            break;
                    }
                }
            }
            else
            {
                SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> FEDAUTHINFO token stream is not long enough to contain the data it claims to.");
                throw SQL.ParsingErrorLength(ParsingErrorState.FedAuthInfoLengthTooShortForData, tokenLen);
            }

            SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo> Processed FEDAUTHINFO token stream: {0}", tempFedAuthInfo);
            if (string.IsNullOrWhiteSpace(tempFedAuthInfo.stsurl) || string.IsNullOrWhiteSpace(tempFedAuthInfo.spn))
            {
                // We should be receiving both stsurl and spn
                SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessFedAuthInfo|ERR> FEDAUTHINFO token stream does not contain both STSURL and SPN.");
                throw SQL.ParsingError(ParsingErrorState.FedAuthInfoDoesNotContainStsurlAndSpn);
            }

            sqlFedAuthInfo = tempFedAuthInfo;
            return true;
        }

        internal static bool TryProcessCollation(TdsParserStateObject stateObj, out SqlCollation collation, SqlCollation cachedCollation)
        {
            if (!stateObj.TryReadUInt32(out uint info))
            {
                collation = null;
                return false;
            }
            if (!stateObj.TryReadByte(out byte sortId))
            {
                collation = null;
                return false;
            }

            if (SqlCollation.Equals(cachedCollation, info, sortId))
            {
                collation = cachedCollation;
            }
            else
            {
                collation = new SqlCollation(info, sortId);
                cachedCollation = collation;
            }

            return true;
        }

        internal static bool TryProcessTceCryptoMetadata(TdsParserStateObject stateObj,
            SqlMetaDataPriv col,
            SqlTceCipherInfoTable cipherTable,
            SqlCommandColumnEncryptionSetting columnEncryptionSetting,
            bool isReturnValue,
            SqlInternalConnectionTds connectionHandler,
            SqlCollation cachedCollation, int defaultCodePage, Encoding defaultEncoding,
            TdsParser parser)
        {
            Debug.Assert(isReturnValue == (cipherTable == null), "Ciphertable is not set iff this is a return value");

            // Read the ordinal into cipher table
            ushort index = 0;
            UInt32 userType;

            // For return values there is not cipher table and no ordinal.
            if (cipherTable != null)
            {
                if (!stateObj.TryReadUInt16(out index))
                {
                    return false;
                }

                // validate the index (ordinal passed)
                if (index >= cipherTable.Size)
                {
                    SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TryProcessTceCryptoMetadata|TCE> Incorrect ordinal received {0}, max tab size: {1}", index, cipherTable.Size);
                    throw SQL.ParsingErrorValue(ParsingErrorState.TceInvalidOrdinalIntoCipherInfoTable, index);
                }
            }

            // Read the user type
            if (!stateObj.TryReadUInt32(out userType))
            {
                return false;
            }

            // Read the base TypeInfo
            col.baseTI = new SqlMetaDataPriv();
            if (!TryProcessTypeInfo(stateObj, col.baseTI, userType, cachedCollation, defaultCodePage, defaultEncoding, parser))
            {
                return false;
            }

            // Read the cipher algorithm Id
            byte cipherAlgorithmId;
            if (!stateObj.TryReadByte(out cipherAlgorithmId))
            {
                return false;
            }

            string cipherAlgorithmName = null;
            if (TdsEnums.CustomCipherAlgorithmId == cipherAlgorithmId)
            {
                // Custom encryption algorithm, read the name
                byte nameSize;
                if (!stateObj.TryReadByte(out nameSize))
                {
                    return false;
                }

                if (!stateObj.TryReadString(nameSize, out cipherAlgorithmName))
                {
                    return false;
                }
            }

            // Read Encryption Type.
            byte encryptionType;
            if (!stateObj.TryReadByte(out encryptionType))
            {
                return false;
            }

            // Read Normalization Rule Version.
            byte normalizationRuleVersion;
            if (!stateObj.TryReadByte(out normalizationRuleVersion))
            {
                return false;
            }

            Debug.Assert(col.cipherMD == null, "col.cipherMD should be null in TryProcessTceCryptoMetadata.");

            // Check if TCE is enable and if it is set the crypto MD for the column.
            // TCE is enabled if the command is set to enabled or to resultset only and this is not a return value
            // or if it is set to use connection setting and the connection has TCE enabled.
            if ((columnEncryptionSetting == SqlCommandColumnEncryptionSetting.Enabled ||
                (columnEncryptionSetting == SqlCommandColumnEncryptionSetting.ResultSetOnly && !isReturnValue)) ||
                (columnEncryptionSetting == SqlCommandColumnEncryptionSetting.UseConnectionSetting &&
                connectionHandler != null && connectionHandler.ConnectionOptions != null &&
                connectionHandler.ConnectionOptions.ColumnEncryptionSetting == SqlConnectionColumnEncryptionSetting.Enabled))
            {
                col.cipherMD = new SqlCipherMetadata(cipherTable != null ? (SqlTceCipherInfoEntry)cipherTable[index] : null,
                                                        index,
                                                        cipherAlgorithmId: cipherAlgorithmId,
                                                        cipherAlgorithmName: cipherAlgorithmName,
                                                        encryptionType: encryptionType,
                                                        normalizationRuleVersion: normalizationRuleVersion);
            }
            else
            {
                // If TCE is disabled mark the MD as not encrypted.
                col.isEncrypted = false;
            }

            return true;
        }

        internal static bool TryProcessUDTMetaData(SqlMetaDataPriv metaData, TdsParserStateObject stateObj)
        {

            ushort shortLength;
            byte byteLength;

            if (!stateObj.TryReadUInt16(out shortLength))
            { // max byte size
                return false;
            }
            metaData.length = shortLength;

            // database name
            if (!stateObj.TryReadByte(out byteLength))
            {
                return false;
            }
            if (metaData.udt is null)
            {
                metaData.udt = new SqlMetaDataUdt();
            }
            if (byteLength != 0)
            {
                if (!stateObj.TryReadString(byteLength, out metaData.udt.DatabaseName))
                {
                    return false;
                }
            }

            // schema name
            if (!stateObj.TryReadByte(out byteLength))
            {
                return false;
            }
            if (byteLength != 0)
            {
                if (!stateObj.TryReadString(byteLength, out metaData.udt.SchemaName))
                {
                    return false;
                }
            }

            // type name
            if (!stateObj.TryReadByte(out byteLength))
            {
                return false;
            }
            if (byteLength != 0)
            {
                if (!stateObj.TryReadString(byteLength, out metaData.udt.TypeName))
                {
                    return false;
                }
            }

            if (!stateObj.TryReadUInt16(out shortLength))
            {
                return false;
            }
            if (shortLength != 0)
            {
                if (!stateObj.TryReadString(shortLength, out metaData.udt.AssemblyQualifiedName))
                {
                    return false;
                }
            }

            return true;
        }

        private static bool TryProcessTypeInfo(TdsParserStateObject stateObj, 
            SqlMetaDataPriv col, UInt32 userType, SqlCollation cachedCollation, int _defaultCodePage, Encoding _defaultEncoding,
            TdsParser parser)
        {
            byte byteLen;
            byte tdsType;
            if (!stateObj.TryReadByte(out tdsType))
            {
                return false;
            }

            if (tdsType == TdsEnums.SQLXMLTYPE)
                col.length = TdsEnums.SQL_USHORTVARMAXLEN;  //Use the same length as other plp datatypes
            else if (IsVarTimeTds(tdsType))
                col.length = 0;  // placeholder until we read the scale, just make sure it's not SQL_USHORTVARMAXLEN
            else if (tdsType == TdsEnums.SQLDATE)
            {
                col.length = 3;
            }
            else
            {
                if (!TdsParserExtensions.TryGetTokenLength(tdsType, stateObj, out col.length))
                {
                    return false;
                }
            }

            col.metaType = MetaType.GetSqlDataType(tdsType, userType, col.length);
            col.type = col.metaType.SqlDbType;
            col.tdsType = (col.IsNullable ? col.metaType.NullableType : col.metaType.TDSType);

            if (TdsEnums.SQLUDT == tdsType)
            {
                if (!TdsParserExtensions.TryProcessUDTMetaData(col, stateObj))
                {
                    return false;
                }
            }

            if (col.length == TdsEnums.SQL_USHORTVARMAXLEN)
            {
                Debug.Assert(tdsType == TdsEnums.SQLXMLTYPE ||
                             tdsType == TdsEnums.SQLBIGVARCHAR ||
                             tdsType == TdsEnums.SQLBIGVARBINARY ||
                             tdsType == TdsEnums.SQLNVARCHAR ||
                             tdsType == TdsEnums.SQLUDT,
                             "Invalid streaming datatype");
                col.metaType = MetaType.GetMaxMetaTypeFromMetaType(col.metaType);
                Debug.Assert(col.metaType.IsLong, "Max datatype not IsLong");
                col.length = int.MaxValue;
                if (tdsType == TdsEnums.SQLXMLTYPE)
                {
                    byte schemapresent;
                    if (!stateObj.TryReadByte(out schemapresent))
                    {
                        return false;
                    }

                    if ((schemapresent & 1) != 0)
                    {
                        if (!stateObj.TryReadByte(out byteLen))
                        {
                            return false;
                        }
                        if (col.xmlSchemaCollection is null)
                        {
                            col.xmlSchemaCollection = new SqlMetaDataXmlSchemaCollection();
                        }
                        if (byteLen != 0)
                        {
                            if (!stateObj.TryReadString(byteLen, out col.xmlSchemaCollection.Database))
                            {
                                return false;
                            }
                        }

                        if (!stateObj.TryReadByte(out byteLen))
                        {
                            return false;
                        }
                        if (byteLen != 0)
                        {
                            if (!stateObj.TryReadString(byteLen, out col.xmlSchemaCollection.OwningSchema))
                            {
                                return false;
                            }
                        }

                        short shortLen;
                        if (!stateObj.TryReadInt16(out shortLen))
                        {
                            return false;
                        }
                        if (byteLen != 0)
                        {
                            if (!stateObj.TryReadString(shortLen, out col.xmlSchemaCollection.Name))
                            {
                                return false;
                            }
                        }
                    }
                }
            }

            if (col.type == SqlDbType.Decimal)
            {
                if (!stateObj.TryReadByte(out col.precision))
                {
                    return false;
                }
                if (!stateObj.TryReadByte(out col.scale))
                {
                    return false;
                }
            }

            if (col.metaType.IsVarTime)
            {
                if (!stateObj.TryReadByte(out col.scale))
                {
                    return false;
                }

                Debug.Assert(0 <= col.scale && col.scale <= 7);

                // calculate actual column length here
                // TODO: variable-length calculation needs to be encapsulated better
                switch (col.metaType.SqlDbType)
                {
                    case SqlDbType.Time:
                        col.length = MetaType.GetTimeSizeFromScale(col.scale);
                        break;
                    case SqlDbType.DateTime2:
                        // Date in number of days (3 bytes) + time
                        col.length = 3 + MetaType.GetTimeSizeFromScale(col.scale);
                        break;
                    case SqlDbType.DateTimeOffset:
                        // Date in days (3 bytes) + offset in minutes (2 bytes) + time
                        col.length = 5 + MetaType.GetTimeSizeFromScale(col.scale);
                        break;

                    default:
                        Debug.Fail("Unknown VariableTime type!");
                        break;
                }
            }

            // read the collation for 7.x servers
            if (col.metaType.IsCharType && (tdsType != TdsEnums.SQLXMLTYPE))
            {
                if (!TdsParserExtensions.TryProcessCollation(stateObj, out col.collation, cachedCollation))
                {
                    return false;
                }

                // UTF8 collation
                if (col.collation.IsUTF8)
                {
                    col.encoding = Encoding.UTF8;
                }
                else
                {
                    int codePage = GetCodePage(col.collation, stateObj, parser);

                    if (codePage == _defaultCodePage)
                    {
                        col.codePage = _defaultCodePage;
                        col.encoding = _defaultEncoding;
                    }
                    else
                    {
                        col.codePage = codePage;
                        col.encoding = System.Text.Encoding.GetEncoding(col.codePage);
                    }
                }
            }

            return true;
        }

        internal static bool TryCommonProcessMetaData(TdsParserStateObject stateObj, _SqlMetaData col, SqlTceCipherInfoTable cipherTable, 
            bool fColMD, SqlCommandColumnEncryptionSetting columnEncryptionSetting, bool IsColumnEncryptionSupported,
            SqlInternalConnectionTds connectionHandler, SqlCollation cachedCollation,
            int _defaultCodePage, Encoding defaultEncoding, TdsParser parser)
        {
            byte byteLen;
            uint userType;

            // read user type - 4 bytes 2005, 2 backwards
            if (!stateObj.TryReadUInt32(out userType))
            {
                return false;
            }

            // read flags and set appropriate flags in structure
            byte flags;
            if (!stateObj.TryReadByte(out flags))
            {
                return false;
            }

            col.Updatability = (byte)((flags & TdsEnums.Updatability) >> 2);
            col.IsNullable = (TdsEnums.Nullable == (flags & TdsEnums.Nullable));
            col.IsIdentity = (TdsEnums.Identity == (flags & TdsEnums.Identity));

            // read second byte of column metadata flags
            if (!stateObj.TryReadByte(out flags))
            {
                return false;
            }

            col.IsColumnSet = (TdsEnums.IsColumnSet == (flags & TdsEnums.IsColumnSet));

            if (fColMD && IsColumnEncryptionSupported)
            {
                col.isEncrypted = (TdsEnums.IsEncrypted == (flags & TdsEnums.IsEncrypted));
            }

            // Read TypeInfo
            if (!TryProcessTypeInfo(stateObj, col, userType, cachedCollation, _defaultCodePage, defaultEncoding, parser))
            {
                return false;
            }

            // Read tablename if present
            if (col.metaType.IsLong && !col.metaType.IsPlp)
            {
                int unusedLen = 0xFFFF;      //We ignore this value
                if (!TryProcessOneTable(stateObj, ref unusedLen, out col.multiPartTableName))
                {
                    return false;
                }
            }

            // Read the TCE column cryptoinfo
            if (fColMD && IsColumnEncryptionSupported && col.isEncrypted)
            {
                // If the column is encrypted, we should have a valid cipherTable
                if (cipherTable != null && !TryProcessTceCryptoMetadata(stateObj, col, cipherTable, columnEncryptionSetting, isReturnValue: false, connectionHandler, cachedCollation,
                    _defaultCodePage, defaultEncoding, parser))
                {
                    return false;
                }
            }

            // Read the column name
            if (!stateObj.TryReadByte(out byteLen))
            {
                return false;
            }
            if (!stateObj.TryReadString(byteLen, out col.column))
            {
                return false;
            }

            // We get too many DONE COUNTs from the server, causing too many StatementCompleted event firings.
            // We only need to fire this event when we actually have a meta data stream with 0 or more rows.
            stateObj.HasReceivedColumnMetadata = true;
            return true;
        }

        internal static int GetCodePage(SqlCollation collation, TdsParserStateObject stateObj, TdsParser tdsParser)
        {
            int codePage = 0;

            if (0 != collation._sortId)
            {
                codePage = TdsEnums.CODE_PAGE_FROM_SORT_ID[collation._sortId];
                Debug.Assert(0 != codePage, "GetCodePage accessed codepage array and produced 0!, sortID =" + ((Byte)(collation._sortId)).ToString((IFormatProvider)null));
            }
            else
            {
                int cultureId = collation.LCID;
                bool success = false;

                try
                {
                    codePage = CultureInfo.GetCultureInfo(cultureId).TextInfo.ANSICodePage;

                    // SqlHot 50001398: CodePage can be zero, but we should defer such errors until
                    //  we actually MUST use the code page (i.e. don't error if no ANSI data is sent).
                    success = true;
                }
                catch (ArgumentException)
                {
                }

                // If we failed, it is quite possible this is because certain culture id's
                // were removed in Win2k and beyond, however Sql Server still supports them.
                // In this case we will mask off the sort id (the leading 1). If that fails,
                // or we have a culture id other than the cases below, we throw an error and
                // throw away the rest of the results.

                //  Sometimes GetCultureInfo will return CodePage 0 instead of throwing.
                //  This should be treated as an error and functionality switches into the following logic.
                if (!success || codePage == 0)
                {
                    switch (cultureId)
                    {
                        case 0x10404: // zh-TW
                        case 0x10804: // zh-CN
                        case 0x10c04: // zh-HK
                        case 0x11004: // zh-SG
                        case 0x11404: // zh-MO
                        case 0x10411: // ja-JP
                        case 0x10412: // ko-KR
                                      // If one of the following special cases, mask out sortId and
                                      // retry.
                            cultureId = cultureId & 0x03fff;

                            try
                            {
                                codePage = new CultureInfo(cultureId).TextInfo.ANSICodePage;
                                success = true;
                            }
                            catch (ArgumentException)
                            {
                            }
                            break;
                        case 0x827:     // Mapping Non-supported Lithuanian code page to supported Lithuanian.
                            try
                            {
                                codePage = new CultureInfo(0x427).TextInfo.ANSICodePage;
                                success = true;
                            }
                            catch (ArgumentException)
                            {
                            }
                            break;
                        case 0x43f:
                            codePage = 1251;  // Kazakh code page based on SQL Server
                            break;
                        case 0x10437:
                            codePage = 1252;  // Georgian code page based on SQL Server
                            break;
                        default:
                            break;
                    }

                    if (!success)
                    {
                        tdsParser.ThrowUnsupportedCollationEncountered(stateObj);
                    }

                    Debug.Assert(codePage >= 0, $"Invalid code page. codePage: {codePage}. cultureId: {cultureId}");
                }
            }

            return codePage;
        }


        /// <summary>
        /// <para> Parses the TDS message to read single CIPHER_INFO entry.</para>
        /// </summary>
        internal static bool TryReadCipherInfoEntry(TdsParserStateObject stateObj, out SqlTceCipherInfoEntry entry)
        {
            byte cekValueCount = 0;
            entry = new SqlTceCipherInfoEntry(ordinal: 0);

            // Read the DB ID
            int dbId;
            if (!stateObj.TryReadInt32(out dbId))
            {
                return false;
            }

            // Read the keyID
            int keyId;
            if (!stateObj.TryReadInt32(out keyId))
            {
                return false;
            }

            // Read the key version
            int keyVersion;
            if (!stateObj.TryReadInt32(out keyVersion))
            {
                return false;
            }

            // Read the key MD Version
            byte[] keyMDVersion = new byte[8];
            if (!stateObj.TryReadByteArray(keyMDVersion, 8))
            {
                return false;
            }

            // Read the value count
            if (!stateObj.TryReadByte(out cekValueCount))
            {
                return false;
            }

            for (int i = 0; i < cekValueCount; i++)
            {
                // Read individual CEK values
                byte[] encryptedCek;
                string keyPath;
                string keyStoreName;
                byte algorithmLength;
                string algorithmName;
                ushort shortValue;
                byte byteValue;
                int length;

                // Read the length of encrypted CEK
                if (!stateObj.TryReadUInt16(out shortValue))
                {
                    return false;
                }

                length = shortValue;
                encryptedCek = new byte[length];

                // Read the actual encrypted CEK
                if (!stateObj.TryReadByteArray(encryptedCek, length))
                {
                    return false;
                }

                // Read the length of key store name
                if (!stateObj.TryReadByte(out byteValue))
                {
                    return false;
                }

                length = byteValue;

                // And read the key store name now
                if (!stateObj.TryReadString(length, out keyStoreName))
                {
                    return false;
                }

                // Read the length of key Path
                if (!stateObj.TryReadUInt16(out shortValue))
                {
                    return false;
                }

                length = shortValue;

                // Read the key path string
                if (!stateObj.TryReadString(length, out keyPath))
                {
                    return false;
                }

                // Read the length of the string carrying the encryption algo
                if (!stateObj.TryReadByte(out algorithmLength))
                {
                    return false;
                }

                length = (int)algorithmLength;

                // Read the string carrying the encryption algo  (eg. RSA_PKCS_OAEP)
                if (!stateObj.TryReadString(length, out algorithmName))
                {
                    return false;
                }

                // Add this encrypted CEK blob to our list of encrypted values for the CEK
                entry.Add(encryptedCek,
                    databaseId: dbId,
                    cekId: keyId,
                    cekVersion: keyVersion,
                    cekMdVersion: keyMDVersion,
                    keyPath: keyPath,
                    keyStoreName: keyStoreName,
                    algorithmName: algorithmName);
            }

            return true;
        }

        /// <summary>
        /// <para> Parses the TDS message to read a single CIPHER_INFO table.</para>
        /// </summary>
        internal static bool TryProcessCipherInfoTable(TdsParserStateObject stateObj, out SqlTceCipherInfoTable cipherTable)
        {
            // Read count
            short tableSize = 0;
            cipherTable = null;
            if (!stateObj.TryReadInt16(out tableSize))
            {
                return false;
            }

            if (0 != tableSize)
            {
                SqlTceCipherInfoTable tempTable = new SqlTceCipherInfoTable(tableSize);

                // Read individual entries
                for (int i = 0; i < tableSize; i++)
                {
                    SqlTceCipherInfoEntry entry;
                    if (!TryReadCipherInfoEntry(stateObj, out entry))
                    {
                        return false;
                    }

                    tempTable[i] = entry;
                }

                cipherTable = tempTable;
            }

            return true;
        }


        internal static bool TryProcessMetaData(int cColumns, TdsParserStateObject stateObj, out _SqlMetaDataSet metaData, SqlCommandColumnEncryptionSetting columnEncryptionSetting, bool IsColumnEncryptionSupported,
            SqlInternalConnectionTds connectionHandler, SqlCollation cachedCollation,
            int defaultCodePage, Encoding defaultEncoding, TdsParser parser)
        {
            Debug.Assert(cColumns > 0, "should have at least 1 column in metadata!");

            // Read the cipher info table first
            SqlTceCipherInfoTable cipherTable = null;
            if (IsColumnEncryptionSupported)
            {
                if (!TryProcessCipherInfoTable(stateObj, out cipherTable))
                {
                    metaData = null;
                    return false;
                }
            }

            // Read the ColumnData fields
            _SqlMetaDataSet newMetaData = new _SqlMetaDataSet(cColumns, cipherTable);
            for (int i = 0; i < cColumns; i++)
            {
                if (!TdsParserExtensions.TryCommonProcessMetaData(stateObj, newMetaData[i], cipherTable, fColMD: true, columnEncryptionSetting: columnEncryptionSetting, IsColumnEncryptionSupported,
                    connectionHandler, cachedCollation, defaultCodePage, defaultEncoding, parser))
                {
                    metaData = null;
                    return false;
                }
            }

            // DEVNOTE: cipherTable is discarded at this point since its no longer needed.
            metaData = newMetaData;
            return true;
        }

        internal static bool IsVarTimeTds(byte tdsType) => tdsType == TdsEnums.SQLTIME || tdsType == TdsEnums.SQLDATETIME2 || tdsType == TdsEnums.SQLDATETIMEOFFSET;


        internal static bool TryProcessOneTable(TdsParserStateObject stateObj, ref int length, out MultiPartTableName multiPartTableName)
        {
            ushort tableLen;
            MultiPartTableName mpt;
            string value;

            multiPartTableName = default(MultiPartTableName);

            mpt = new MultiPartTableName();
            byte nParts;

            // Find out how many parts in the TDS stream
            if (!stateObj.TryReadByte(out nParts))
            {
                return false;
            }
            length--;
            if (nParts == 4)
            {
                if (!stateObj.TryReadUInt16(out tableLen))
                {
                    return false;
                }
                length -= 2;
                if (!stateObj.TryReadString(tableLen, out value))
                {
                    return false;
                }
                mpt.ServerName = value;
                nParts--;
                length -= (tableLen * 2); // wide bytes
            }
            if (nParts == 3)
            {
                if (!stateObj.TryReadUInt16(out tableLen))
                {
                    return false;
                }
                length -= 2;
                if (!stateObj.TryReadString(tableLen, out value))
                {
                    return false;
                }
                mpt.CatalogName = value;
                length -= (tableLen * 2); // wide bytes
                nParts--;
            }
            if (nParts == 2)
            {
                if (!stateObj.TryReadUInt16(out tableLen))
                {
                    return false;
                }
                length -= 2;
                if (!stateObj.TryReadString(tableLen, out value))
                {
                    return false;
                }
                mpt.SchemaName = value;
                length -= (tableLen * 2); // wide bytes
                nParts--;
            }
            if (nParts == 1)
            {
                if (!stateObj.TryReadUInt16(out tableLen))
                {
                    return false;
                }
                length -= 2;
                if (!stateObj.TryReadString(tableLen, out value))
                {
                    return false;
                }
                mpt.TableName = value;
                length -= (tableLen * 2); // wide bytes
                nParts--;
            }
            Debug.Assert(nParts == 0, "ProcessTableName:Unidentified parts in the table name token stream!");

            multiPartTableName = mpt;
            return true;
        }

        internal static bool TryProcessTableName(int length, TdsParserStateObject stateObj, out MultiPartTableName[] multiPartTableNames)
        {
            int tablesAdded = 0;

            MultiPartTableName[] tables = new MultiPartTableName[1];
            MultiPartTableName mpt;
            while (length > 0)
            {
                if (!TdsParserExtensions.TryProcessOneTable(stateObj, ref length, out mpt))
                {
                    multiPartTableNames = null;
                    return false;
                }
                if (tablesAdded == 0)
                {
                    tables[tablesAdded] = mpt;
                }
                else
                {
                    MultiPartTableName[] newTables = new MultiPartTableName[tables.Length + 1];
                    Array.Copy(tables, 0, newTables, 0, tables.Length);
                    newTables[tables.Length] = mpt;
                    tables = newTables;
                }

                tablesAdded++;
            }

            multiPartTableNames = tables;
            return true;
        }

        

        /// <summary>
        /// Determines if a column value should be transparently decrypted (based on SqlCommand and Connection String settings).
        /// </summary>
        /// <returns>true if the value should be transparently decrypted, false otherwise</returns>
        internal static bool ShouldHonorTceForRead(SqlCommandColumnEncryptionSetting columnEncryptionSetting, SqlInternalConnectionTds connection)
        {
            // Command leve setting trumps all
            switch (columnEncryptionSetting)
            {
                case SqlCommandColumnEncryptionSetting.Disabled:
                    return false;
                case SqlCommandColumnEncryptionSetting.Enabled:
                    return true;
                case SqlCommandColumnEncryptionSetting.ResultSetOnly:
                    return true;
                default:
                    // Check connection level setting!
                    Debug.Assert(SqlCommandColumnEncryptionSetting.UseConnectionSetting == columnEncryptionSetting,
                        "Unexpected value for command level override");
                    return (connection != null && connection.ConnectionOptions != null && connection.ConnectionOptions.ColumnEncryptionSetting == SqlConnectionColumnEncryptionSetting.Enabled);
            }
        }

        internal static object GetNullSqlValue(SqlBuffer nullVal, SqlMetaDataPriv md, SqlCommandColumnEncryptionSetting columnEncryptionSetting, SqlInternalConnectionTds connection)
        {
            SqlDbType type = md.type;

            if (type == SqlDbType.VarBinary && // if its a varbinary
                md.isEncrypted &&// and encrypted
                ShouldHonorTceForRead(columnEncryptionSetting, connection))
            {
                type = md.baseTI.type; // the use the actual (plaintext) type
            }

            switch (type)
            {
                case SqlDbType.Real:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Single);
                    break;

                case SqlDbType.Float:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Double);
                    break;

                case SqlDbType.Udt:
                case SqlDbType.Binary:
                case SqlDbType.VarBinary:
                case SqlDbType.Image:
                    nullVal.SqlBinary = SqlBinary.Null;
                    break;

                case SqlDbType.UniqueIdentifier:
                    nullVal.SqlGuid = SqlGuid.Null;
                    break;

                case SqlDbType.Bit:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Boolean);
                    break;

                case SqlDbType.TinyInt:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Byte);
                    break;

                case SqlDbType.SmallInt:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Int16);
                    break;

                case SqlDbType.Int:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Int32);
                    break;

                case SqlDbType.BigInt:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Int64);
                    break;

                case SqlDbType.Char:
                case SqlDbType.VarChar:
                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.NText:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.String);
                    break;

                case SqlDbType.Decimal:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Decimal);
                    break;

                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.DateTime);
                    break;

                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Money);
                    break;

                case SqlDbType.Variant:
                    // DBNull.Value will have to work here
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Empty);
                    break;

                case SqlDbType.Xml:
                    nullVal.SqlCachedBuffer = SqlCachedBuffer.Null;
                    break;

                case SqlDbType.Date:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Date);
                    break;

                case SqlDbType.Time:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.Time);
                    break;

                case SqlDbType.DateTime2:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.DateTime2);
                    break;

                case SqlDbType.DateTimeOffset:
                    nullVal.SetToNullOfType(SqlBuffer.StorageType.DateTimeOffset);
                    break;

                case SqlDbType.Timestamp:
                    if (!LocalAppContextSwitches.LegacyRowVersionNullBehavior)
                    {
                        nullVal.SetToNullOfType(SqlBuffer.StorageType.SqlBinary);
                    }
                    break;

                default:
                    Debug.Fail("unknown null sqlType!" + md.type.ToString());
                    break;
            }

            return nullVal;
        }


        internal static bool IsNull(MetaType mt, ulong length)
        {
            // null bin and char types have a length of -1 to represent null
            if (mt.IsPlp)
            {
                return (TdsEnums.SQL_PLP_NULL == length);
            }

            // HOTFIX #50000415: for image/text, 0xFFFF is the length, not representing null
            if ((TdsEnums.VARNULL == length) && !mt.IsLong)
            {
                return true;
            }

            // other types have a length of 0 to represent null
            // long and non-PLP types will always return false because these types are either char or binary
            // this is expected since for long and non-plp types isnull is checked based on textptr field and not the length
            return ((TdsEnums.FIXEDNULL == length) && !mt.IsCharType && !mt.IsBinType);
        }


        internal static bool TrySkipRow(_SqlMetaDataSet columns, TdsParserStateObject stateObj)
        {
            return TrySkipRow(columns, 0, stateObj);
        }

        internal static bool TrySkipRow(_SqlMetaDataSet columns, int startCol, TdsParserStateObject stateObj)
        {
            for (int i = startCol; i < columns.Length; i++)
            {
                _SqlMetaData md = columns[i];

                if (!TrySkipValue(md, i, stateObj))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// This method skips bytes of a single column value from the media. It supports NBCROW and handles all types of values, including PLP and long
        /// </summary>
        internal static bool TrySkipValue(SqlMetaDataPriv md, int columnOrdinal, TdsParserStateObject stateObj)
        {
            if (stateObj.IsNullCompressionBitSet(columnOrdinal))
            {
                return true;
            }

            if (md.metaType.IsPlp)
            {
                ulong ignored;
                if (!stateObj.TrySkipPlpValue(ulong.MaxValue, out ignored))
                {
                    return false;
                }
            }
            else if (md.metaType.IsLong)
            {
                Debug.Assert(!md.metaType.IsPlp, "Plp types must be handled using SkipPlpValue");

                byte textPtrLen;
                if (!stateObj.TryReadByte(out textPtrLen))
                {
                    return false;
                }

                if (0 != textPtrLen)
                {
                    if (!stateObj.TrySkipBytes(textPtrLen + TdsEnums.TEXT_TIME_STAMP_LEN))
                    {
                        return false;
                    }

                    int length;
                    if (!TdsParserExtensions.TryGetTokenLength(md.tdsType, stateObj, out length))
                    {
                        return false;
                    }
                    if (!stateObj.TrySkipBytes(length))
                    {
                        return false;
                    }
                }
            }
            else
            {
                int length;
                if (!TdsParserExtensions.TryGetTokenLength(md.tdsType, stateObj, out length))
                {
                    return false;
                }

                // if false, no value to skip - it's null
                if (!TdsParserExtensions.IsNull(md.metaType, (ulong)length))
                {
                    if (!stateObj.TrySkipBytes(length))
                    {
                        return false;
                    }
                }
            }

            return true;
        }


    }
}
