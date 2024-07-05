// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.Common;
using Microsoft.Data.SqlClient.Server;

namespace Microsoft.Data.SqlClient
{
    // TdsValueSetter handles writing a single value out to a TDS stream
    //   This class can easily be extended to handle multiple versions of TDS by sub-classing and virtualizing
    //   methods that have different formats or are not supported in one or the other version.
    internal class TdsValueSetter
    {
        #region Private fields

        private TdsParserStateObject _stateObj;      // target to write to
        private SmiMetaData _metaData;      // metadata describing value
        private bool _isPlp;         // should this column be sent in PLP format?
        private bool _plpUnknownSent;// did we send initial UNKNOWN_LENGTH marker?
        private Encoder _encoder;       // required for chunking character type data
        private SmiMetaData _variantType;   // required for sql_variant
#if DEBUG
        private int _currentOffset; // for chunking, verify that caller is using correct offsets
#endif

        #endregion

        #region Exposed Construct/factory methods

        internal TdsValueSetter(TdsParserStateObject stateObj, SmiMetaData md)
        {
            _stateObj = stateObj;
            _metaData = md;
            _isPlp = MetaDataUtilsSmi.IsPlpFormat(md);
            _plpUnknownSent = false;
            _encoder = null;
#if DEBUG
            _currentOffset = 0;
#endif
        }

        #endregion

        #region Setters

        // Set value to null
        //  valid for all types
        internal void SetDBNull()
        {
            Debug.Assert(!_plpUnknownSent, "Setting a column to null that we already stated sending!");
            if (_isPlp)
            {
                TdsParserExtensions.WriteUnsignedLong(TdsEnums.SQL_PLP_NULL, _stateObj);
            }
            else
            {
                switch (_metaData.SqlDbType)
                {
                    case SqlDbType.BigInt:
                    case SqlDbType.Bit:
                    case SqlDbType.DateTime:
                    case SqlDbType.Decimal:
                    case SqlDbType.Float:
                    case SqlDbType.Int:
                    case SqlDbType.Money:
                    case SqlDbType.Real:
                    case SqlDbType.UniqueIdentifier:
                    case SqlDbType.SmallDateTime:
                    case SqlDbType.SmallInt:
                    case SqlDbType.SmallMoney:
                    case SqlDbType.TinyInt:
                    case SqlDbType.Date:
                    case SqlDbType.Time:
                    case SqlDbType.DateTime2:
                    case SqlDbType.DateTimeOffset:
                        _stateObj.WriteByte(TdsEnums.FIXEDNULL);
                        break;
                    case SqlDbType.Binary:
                    case SqlDbType.Char:
                    case SqlDbType.Image:
                    case SqlDbType.NChar:
                    case SqlDbType.NText:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Text:
                    case SqlDbType.Timestamp:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                        TdsParserExtensions.WriteShort(TdsEnums.VARNULL, _stateObj);
                        break;
                    case SqlDbType.Udt:
                    case SqlDbType.Xml:
                        Debug.Fail("PLP-only types shouldn't get to this point. Type: " + _metaData.SqlDbType);
                        break;
                    case SqlDbType.Variant:
                        TdsParserExtensions.WriteInt(TdsEnums.FIXEDNULL, _stateObj);
                        break;
                    case SqlDbType.Structured:
                        Debug.Fail("Not yet implemented.  Not needed until Structured UDTs");
                        break;
                    default:
                        Debug.Fail("Unexpected SqlDbType: " + _metaData.SqlDbType);
                        break;
                }
            }
        }

        //  valid for SqlDbType.Bit
        internal void SetBoolean(bool value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetBoolean));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(3, TdsEnums.SQLBIT, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            if (value)
            {
                _stateObj.WriteByte(1);
            }
            else
            {
                _stateObj.WriteByte(0);
            }
        }

        //  valid for SqlDbType.TinyInt
        internal void SetByte(byte value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetByte));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(3, TdsEnums.SQLINT1, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            _stateObj.WriteByte(value);
        }

        // Semantics for SetBytes are to modify existing value, not overwrite
        //  Use in combination with SetLength to ensure overwriting when necessary
        // valid for SqlDbTypes: Binary, VarBinary, Image, Udt, Xml
        //      (VarBinary assumed for variants)
        internal int SetBytes(long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetBytes));
            CheckSettingOffset(fieldOffset);

            SetBytesNoOffsetHandling(fieldOffset, buffer, bufferOffset, length);
#if DEBUG
            _currentOffset += length;
#endif
            return length;
        }

        private void SetBytesNoOffsetHandling(long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            if (_isPlp)
            {
                if (!_plpUnknownSent)
                {
                    TdsParserExtensions.WriteUnsignedLong(TdsEnums.SQL_PLP_UNKNOWNLEN, _stateObj);
                    _plpUnknownSent = true;
                }

                // Write chunk length & chunk
                TdsParserExtensions.WriteInt(length, _stateObj);
                _stateObj.WriteByteArray(buffer, length, bufferOffset);
            }
            else
            {
                // Non-plp data must be sent in one chunk for now.
#if DEBUG
                Debug.Assert(0 == _currentOffset, "SetBytes doesn't yet support chunking for non-plp data: " + _currentOffset);

#endif
                Debug.Assert(!MetaType.GetMetaTypeFromSqlDbType(_metaData.SqlDbType, _metaData.IsMultiValued).IsLong,
                    "We're assuming long length types are sent as PLP. SqlDbType = " + _metaData.SqlDbType);

                if (SqlDbType.Variant == _metaData.SqlDbType)
                {
                    TdsParserExtensions.WriteSqlVariantHeader(4 + length, TdsEnums.SQLBIGVARBINARY, 2, _stateObj);
                }
                TdsParserExtensions.WriteShort(length, _stateObj);
                _stateObj.WriteByteArray(buffer, length, bufferOffset);
            }
        }

        internal void SetBytesLength(long length)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetBytes));
            CheckSettingOffset(length);

            if (0 == length)
            {
                if (_isPlp)
                {
                    Debug.Assert(!_plpUnknownSent, "A plpUnknown has already been sent before setting length to zero.");

                    TdsParserExtensions.WriteLong(0, _stateObj);
                    _plpUnknownSent = true;
                }
                else
                {
                    Debug.Assert(!MetaType.GetMetaTypeFromSqlDbType(_metaData.SqlDbType, _metaData.IsMultiValued).IsLong,
                        "We're assuming long length types are sent as PLP. SqlDbType = " + _metaData.SqlDbType);

                    if (SqlDbType.Variant == _metaData.SqlDbType)
                    {
                        TdsParserExtensions.WriteSqlVariantHeader(4, TdsEnums.SQLBIGVARBINARY, 2, _stateObj);
                    }
                    TdsParserExtensions.WriteShort(0, _stateObj);
                }
            }
            if (_plpUnknownSent)
            {
                TdsParserExtensions.WriteInt(TdsEnums.SQL_PLP_CHUNK_TERMINATOR, _stateObj);
                _plpUnknownSent = false;
            }

#if DEBUG
            _currentOffset = 0;
#endif
        }

        // Semantics for SetChars are to modify existing value, not overwrite
        //  Use in combination with SetLength to ensure overwriting when necessary
        // valid for character types: Char, VarChar, Text, NChar, NVarChar, NText
        //      (NVarChar and global clr collation assumed for variants)
        internal int SetChars(long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetChars));

            // ANSI types must convert to byte[] because that's the tool we have.
            if (MetaDataUtilsSmi.IsAnsiType(_metaData.SqlDbType))
            {
                if (null == _encoder)
                {
                    _encoder = _stateObj.Parser._defaultEncoding.GetEncoder();
                }
                byte[] bytes = new byte[_encoder.GetByteCount(buffer, bufferOffset, length, false)];
                _encoder.GetBytes(buffer, bufferOffset, length, bytes, 0, false);
                SetBytesNoOffsetHandling(fieldOffset, bytes, 0, bytes.Length);
            }
            else
            {
                CheckSettingOffset(fieldOffset);

                // Send via PLP format if we can.
                if (_isPlp)
                {
                    // Handle initial PLP markers
                    if (!_plpUnknownSent)
                    {
                        TdsParserExtensions.WriteUnsignedLong(TdsEnums.SQL_PLP_UNKNOWNLEN, _stateObj);
                        _plpUnknownSent = true;
                    }

                    // Write chunk length
                    TdsParserExtensions.WriteInt(length * ADP.CharSize, _stateObj);
                    TdsParserExtensions.WriteCharArray(buffer, length, bufferOffset, _stateObj);
                }
                else
                {
                    // Non-plp data must be sent in one chunk for now.
#if DEBUG
                    Debug.Assert(0 == _currentOffset, "SetChars doesn't yet support chunking for non-plp data: " + _currentOffset);
#endif

                    if (SqlDbType.Variant == _metaData.SqlDbType)
                    {
                        _stateObj.Parser.WriteSqlVariantValue(new string(buffer, bufferOffset, length), length, 0, _stateObj);
                    }
                    else
                    {
                        Debug.Assert(!MetaType.GetMetaTypeFromSqlDbType(_metaData.SqlDbType, _metaData.IsMultiValued).IsLong,
                                "We're assuming long length types are sent as PLP. SqlDbType = " + _metaData.SqlDbType);
                        TdsParserExtensions.WriteShort(length * ADP.CharSize, _stateObj);
                        TdsParserExtensions.WriteCharArray(buffer, length, bufferOffset, _stateObj);
                    }
                }
            }

#if DEBUG
            _currentOffset += length;
#endif
            return length;
        }
        internal void SetCharsLength(long length)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetChars));
            CheckSettingOffset(length);

            if (0 == length)
            {
                if (_isPlp)
                {
                    Debug.Assert(!_plpUnknownSent, "A plpUnknown has already been sent before setting length to zero.");

                    TdsParserExtensions.WriteLong(0, _stateObj);
                    _plpUnknownSent = true;
                }
                else
                {
                    Debug.Assert(!MetaType.GetMetaTypeFromSqlDbType(_metaData.SqlDbType, _metaData.IsMultiValued).IsLong,
                        "We're assuming long length types are sent as PLP. SqlDbType = " + _metaData.SqlDbType);

                    TdsParserExtensions.WriteShort(0, _stateObj);
                }
            }
            if (_plpUnknownSent)
            {
                TdsParserExtensions.WriteInt(TdsEnums.SQL_PLP_CHUNK_TERMINATOR, _stateObj);
                _plpUnknownSent = false;
            }
            _encoder = null;

#if DEBUG
            _currentOffset = 0;
#endif
        }

        // valid for character types: Char, VarChar, Text, NChar, NVarChar, NText
        internal void SetString(string value, int offset, int length)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetString));

            // ANSI types must convert to byte[] because that's the tool we have.
            if (MetaDataUtilsSmi.IsAnsiType(_metaData.SqlDbType))
            {
                byte[] bytes;
                // Optimize for common case of writing entire string
                if (offset == 0 && value.Length <= length)
                {
                    bytes = _stateObj.Parser._defaultEncoding.GetBytes(value);
                }
                else
                {
                    char[] chars = value.ToCharArray(offset, length);
                    bytes = _stateObj.Parser._defaultEncoding.GetBytes(chars);
                }
                SetBytes(0, bytes, 0, bytes.Length);
                SetBytesLength(bytes.Length);
            }
            else if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                Debug.Assert(null != _variantType && SqlDbType.NVarChar == _variantType.SqlDbType, "Invalid variant type");

                SqlCollation collation = SqlCollation.FromLCIDAndSort(checked((int)_variantType.LocaleId), _variantType.CompareOptions);

                if (length * ADP.CharSize > TdsEnums.TYPE_SIZE_LIMIT)
                { // send as varchar for length greater than 4000
                    byte[] bytes;
                    // Optimize for common case of writing entire string
                    if (offset == 0 && value.Length <= length)
                    {
                        bytes = _stateObj.Parser._defaultEncoding.GetBytes(value);
                    }
                    else
                    {
                        bytes = _stateObj.Parser._defaultEncoding.GetBytes(value.ToCharArray(offset, length));
                    }
                    TdsParserExtensions.WriteSqlVariantHeader(9 + bytes.Length, TdsEnums.SQLBIGVARCHAR, 7, _stateObj);
                    TdsParserExtensions.WriteUnsignedInt(collation._info, _stateObj); // propbytes: collation.Info
                    _stateObj.WriteByte(collation._sortId); // propbytes: collation.SortId
                    TdsParserExtensions.WriteShort(bytes.Length, _stateObj); // propbyte: varlen
                    _stateObj.WriteByteArray(bytes, bytes.Length, 0);
                }
                else
                {
                    TdsParserExtensions.WriteSqlVariantHeader(9 + length * ADP.CharSize, TdsEnums.SQLNVARCHAR, 7, _stateObj);
                    TdsParserExtensions.WriteUnsignedInt(collation._info, _stateObj); // propbytes: collation.Info
                    _stateObj.WriteByte(collation._sortId); // propbytes: collation.SortId
                    TdsParserExtensions.WriteShort(length * ADP.CharSize, _stateObj); // propbyte: varlen
                    TdsParserExtensions.WriteString(value, length, offset, _stateObj);
                }
                _variantType = null;
            }
            else if (_isPlp)
            {
                // Send the string as a complete PLP chunk.
                TdsParserExtensions.WriteLong(length * ADP.CharSize, _stateObj);  // PLP total length
                TdsParserExtensions.WriteInt(length * ADP.CharSize, _stateObj);   // Chunk length
                TdsParserExtensions.WriteString(value, length, offset, _stateObj);  // Data
                if (length != 0)
                {
                    TdsParserExtensions.WriteInt(TdsEnums.SQL_PLP_CHUNK_TERMINATOR, _stateObj); // Terminator
                }
            }
            else
            {
                TdsParserExtensions.WriteShort(length * ADP.CharSize, _stateObj);
                TdsParserExtensions.WriteString(value, length, offset, _stateObj);
            }
        }

        // valid for SqlDbType.SmallInt
        internal void SetInt16(short value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetInt16));

            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(4, TdsEnums.SQLINT2, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            TdsParserExtensions.WriteShort(value, _stateObj);
        }

        // valid for SqlDbType.Int
        internal void SetInt32(int value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetInt32));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(6, TdsEnums.SQLINT4, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            TdsParserExtensions.WriteInt(value, _stateObj);
        }

        // valid for SqlDbType.BigInt, SqlDbType.Money, SqlDbType.SmallMoney
        internal void SetInt64(long value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetInt64));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                if (null == _variantType)
                {
                    TdsParserExtensions.WriteSqlVariantHeader(10, TdsEnums.SQLINT8, 0, _stateObj);
                    TdsParserExtensions.WriteLong(value, _stateObj);
                }
                else
                {
                    Debug.Assert(SqlDbType.Money == _variantType.SqlDbType, "Invalid variant type");

                    TdsParserExtensions.WriteSqlVariantHeader(10, TdsEnums.SQLMONEY, 0, _stateObj);
                    TdsParserExtensions.WriteInt((int)(value >> 0x20), _stateObj);
                    TdsParserExtensions.WriteInt((int)value, _stateObj);
                    _variantType = null;
                }
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
                if (SqlDbType.SmallMoney == _metaData.SqlDbType)
                {
                    TdsParserExtensions.WriteInt((int)value, _stateObj);
                }
                else if (SqlDbType.Money == _metaData.SqlDbType)
                {
                    TdsParserExtensions.WriteInt((int)(value >> 0x20), _stateObj);
                    TdsParserExtensions.WriteInt((int)value, _stateObj);
                }
                else
                {
                    TdsParserExtensions.WriteLong(value, _stateObj);
                }
            }
        }

        // valid for SqlDbType.Real
        internal void SetSingle(float value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetSingle));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(6, TdsEnums.SQLFLT4, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            TdsParserExtensions.WriteFloat(value, _stateObj);
        }

        // valid for SqlDbType.Float
        internal void SetDouble(double value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetDouble));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(10, TdsEnums.SQLFLT8, 0, _stateObj);
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
            TdsParserExtensions.WriteDouble(value, _stateObj);
        }

        // valid for SqlDbType.Numeric (uses SqlDecimal since Decimal cannot hold full range)
        internal void SetSqlDecimal(SqlDecimal value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetSqlDecimal));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(21, TdsEnums.SQLNUMERICN, 2, _stateObj);
                _stateObj.WriteByte(value.Precision); // propbytes: precision
                _stateObj.WriteByte(value.Scale); // propbytes: scale
                TdsParserExtensions.WriteSqlDecimal(value, _stateObj);
            }
            else
            {
                _stateObj.WriteByte(checked((byte)MetaType.MetaDecimal.FixedLength)); // SmiMetaData's length and actual wire format's length are different
                TdsParserExtensions.WriteSqlDecimal(SqlDecimal.ConvertToPrecScale(value, _metaData.Precision, _metaData.Scale), _stateObj);
            }
        }

        // valid for DateTime, SmallDateTime, Date, DateTime2
        internal void SetDateTime(DateTime value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetDateTime));
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                if ((_variantType != null) && (_variantType.SqlDbType == SqlDbType.DateTime2))
                {
                    TdsParserExtensions.WriteSqlVariantDateTime2(value, _stateObj);
                }
                else if ((_variantType != null) && (_variantType.SqlDbType == SqlDbType.Date))
                {
                    TdsParserExtensions.WriteSqlVariantDate(value, _stateObj);
                }
                else
                {
                    TdsDateTime dt = MetaType.FromDateTime(value, 8);
                    TdsParserExtensions.WriteSqlVariantHeader(10, TdsEnums.SQLDATETIME, 0, _stateObj);
                    TdsParserExtensions.WriteInt(dt.days, _stateObj);
                    TdsParserExtensions.WriteInt(dt.time, _stateObj);
                }

                // Clean the variant metadata to prevent sharing it with next row.
                // As a reminder, SetVariantType raises an assert if _variantType is not clean
                _variantType = null;
            }
            else
            {
                _stateObj.WriteByte((byte)_metaData.MaxLength);
                if (SqlDbType.SmallDateTime == _metaData.SqlDbType)
                {
                    TdsDateTime dt = MetaType.FromDateTime(value, (byte)_metaData.MaxLength);
                    Debug.Assert(0 <= dt.days && dt.days <= ushort.MaxValue, "Invalid DateTime '" + value + "' for SmallDateTime");

                    TdsParserExtensions.WriteShort(dt.days, _stateObj);
                    TdsParserExtensions.WriteShort(dt.time, _stateObj);
                }
                else if (SqlDbType.DateTime == _metaData.SqlDbType)
                {
                    TdsDateTime dt = MetaType.FromDateTime(value, (byte)_metaData.MaxLength);
                    TdsParserExtensions.WriteInt(dt.days, _stateObj);
                    TdsParserExtensions.WriteInt(dt.time, _stateObj);
                }
                else
                { // date and datetime2
                    int days = value.Subtract(DateTime.MinValue).Days;
                    if (SqlDbType.DateTime2 == _metaData.SqlDbType)
                    {
                        long time = value.TimeOfDay.Ticks / TdsEnums.TICKS_FROM_SCALE[_metaData.Scale];
#if NET6_0_OR_GREATER
                        Span<byte> result_time = stackalloc byte[8];
                        BinaryPrimitives.WriteInt64LittleEndian(result_time, time);
                        _stateObj.WriteByteSpan(result_time.Slice(0, (int)_metaData.MaxLength - 3));
#else
                        _stateObj.WriteByteArray(BitConverter.GetBytes(time), (int)_metaData.MaxLength - 3, 0);
#endif
                    }
#if NET6_0_OR_GREATER
                    Span<byte> result_date = stackalloc byte[4];
                    BinaryPrimitives.WriteInt32LittleEndian(result_date, days);
                    _stateObj.WriteByteSpan(result_date.Slice(0, 3));
#else
                    _stateObj.WriteByteArray(BitConverter.GetBytes(days), 3, 0);
#endif
                }
            }
        }

        // valid for UniqueIdentifier
        internal void SetGuid(Guid value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetGuid));

#if NET6_0_OR_GREATER

            Span<byte> bytes = stackalloc byte[16];
            value.TryWriteBytes(bytes);
#else
            byte[] bytes = value.ToByteArray();
#endif
            Debug.Assert(SmiMetaData.DefaultUniqueIdentifier.MaxLength == bytes.Length, "Invalid length for guid bytes: " + bytes.Length);

            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                TdsParserExtensions.WriteSqlVariantHeader(18, TdsEnums.SQLUNIQUEID, 0, _stateObj);
            }
            else
            {
                Debug.Assert(_metaData.MaxLength == bytes.Length, "Unexpected uniqueid metadata length: " + _metaData.MaxLength);

                _stateObj.WriteByte((byte)_metaData.MaxLength);
            }
#if NET6_0_OR_GREATER
            _stateObj.WriteByteSpan(bytes);
#else
            _stateObj.WriteByteArray(bytes, bytes.Length, 0);
#endif
        }

        // valid for SqlDbType.Time
        internal void SetTimeSpan(TimeSpan value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetTime));
            byte scale;
            byte length;
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                scale = SmiMetaData.DefaultTime.Scale;
                length = (byte)SmiMetaData.DefaultTime.MaxLength;
                TdsParserExtensions.WriteSqlVariantHeader(8, TdsEnums.SQLTIME, 1, _stateObj);
                _stateObj.WriteByte(scale); //propbytes: scale
            }
            else
            {
                scale = _metaData.Scale;
                length = (byte)_metaData.MaxLength;
                _stateObj.WriteByte(length);
            }
            long time = value.Ticks / TdsEnums.TICKS_FROM_SCALE[scale];
#if NET6_0_OR_GREATER
            Span<byte> result_time = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(result_time, time);
            _stateObj.WriteByteSpan(result_time.Slice(0, length));
#else
            _stateObj.WriteByteArray(BitConverter.GetBytes(time), length, 0);
#endif
        }

        // valid for DateTimeOffset
        internal void SetDateTimeOffset(DateTimeOffset value)
        {
            Debug.Assert(
                SmiXetterAccessMap.IsSetterAccessValid(_metaData, SmiXetterTypeCode.XetDateTimeOffset));
            byte scale;
            byte length;
            if (SqlDbType.Variant == _metaData.SqlDbType)
            {
                // VSTFDevDiv #885208 - DateTimeOffset throws ArgumentException for when passing DateTimeOffset value to a sql_variant TVP
                //                      using a SqlDataRecord or SqlDataReader
                SmiMetaData dateTimeOffsetMetaData = SmiMetaData.DefaultDateTimeOffset;
                scale = MetaType.MetaDateTimeOffset.Scale;
                length = (byte)dateTimeOffsetMetaData.MaxLength;
                TdsParserExtensions.WriteSqlVariantHeader(13, TdsEnums.SQLDATETIMEOFFSET, 1, _stateObj);
                _stateObj.WriteByte(scale); //propbytes: scale
            }
            else
            {
                scale = _metaData.Scale;
                length = (byte)_metaData.MaxLength;
                _stateObj.WriteByte(length);
            }
            DateTime utcDateTime = value.UtcDateTime;
            long time = utcDateTime.TimeOfDay.Ticks / TdsEnums.TICKS_FROM_SCALE[scale];
            int days = utcDateTime.Subtract(DateTime.MinValue).Days;
            short offset = (short)value.Offset.TotalMinutes;

#if NET6_0_OR_GREATER
            // In TDS protocol: 
            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d
            //
            //   date is represented as one 3 - byte unsigned integer that represents the number of days since January 1, year 1.
            //
            //   time(n) is represented as one unsigned integer that represents the number of 10^-n,
            //   (10 to the power of negative n), second increments since 12 AM within a day.
            //     The length, in bytes, of that integer depends on the scale n as follows:
            //       3 bytes if 0 <= n < = 2.
            //       4 bytes if 3 <= n < = 4.
            //       5 bytes if 5 <= n < = 7.
            //     For example:
            //       DateTimeOffset dateTimeOffset = new DateTimeOffset(2024, 1, 1, 23, 59, 59, TimeSpan.Zero);  // using scale of 0
            //       time = 23:59:59, scale is 1, is represented as 863990 in 3 bytes or { 246, 46, 13, 0, 0, 0, 0, 0 } in bytes array

            Span<byte> result = stackalloc byte[8];

            // https://learn.microsoft.com/en-us/dotnet/api/system.buffers.binary.binaryprimitives.writeint64bigendian?view=net-8.0
            // WriteInt64LittleEndian requires 8 bytes to write the value.
            BinaryPrimitives.WriteInt64LittleEndian(result, time);
            // The DateTimeOffset length is variable depending on the scale, 1 to 7, used.
            // If length = 8, 8 - 5 = 3 bytes is used for time.
            // If length = 10, 10 - 5 = 5 bytes is used for time.
            _stateObj.WriteByteSpan(result.Slice(0, length - 5)); // this writes the time value to the state object using dynamic length based on the scale.
            
            // Date is represented as 3 bytes. So, 3 bytes are written to the state object.
            BinaryPrimitives.WriteInt32LittleEndian(result, days);
            _stateObj.WriteByteSpan(result.Slice(0, 3));
#else
            _stateObj.WriteByteArray(BitConverter.GetBytes(time), length - 5, 0); // time
            _stateObj.WriteByteArray(BitConverter.GetBytes(days), 3, 0); // date
#endif
            _stateObj.WriteByte((byte)(offset & 0xff)); // offset byte 1
            _stateObj.WriteByte((byte)((offset >> 8) & 0xff)); // offset byte 2
        }

        internal void SetVariantType(SmiMetaData value)
        {
            Debug.Assert(null == _variantType, "Variant type can only be set once");
            Debug.Assert(value != null &&
                (value.SqlDbType == SqlDbType.Money ||
                value.SqlDbType == SqlDbType.NVarChar ||
                value.SqlDbType == SqlDbType.Date ||
                value.SqlDbType == SqlDbType.DateTime ||
                value.SqlDbType == SqlDbType.DateTime2 ||
                value.SqlDbType == SqlDbType.DateTimeOffset ||
                value.SqlDbType == SqlDbType.SmallDateTime
                ), "Invalid variant type");
            _variantType = value;
        }

        #endregion

        #region private methods
        [Conditional("DEBUG")]
        private void CheckSettingOffset(long offset)
        {
#if DEBUG
            Debug.Assert(offset == _currentOffset, "Invalid offset passed. Should be: " + _currentOffset + ", but was: " + offset);
#endif
        }
        #endregion
    }
}
