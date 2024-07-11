// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
#if NETFRAMEWORK
using System.Data.OleDb;
#endif
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Xml;
using Microsoft.Data.Common;
using Microsoft.SqlServer.Server;

namespace Microsoft.Data.SqlClient
{
    internal sealed class MetaType
    {
#if NET6_0_OR_GREATER
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
#endif
        internal readonly Type ClassType;   // com+ type
#if NET6_0_OR_GREATER
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
#endif
        internal readonly Type SqlType;

        internal readonly int FixedLength; // fixed length size in bytes (-1 for variable)
        internal readonly bool IsFixed;     // true if fixed length, note that sqlchar and sqlbinary are not considered fixed length
        internal readonly bool IsLong;      // true if long
        internal readonly bool IsPlp;       // Column is Partially Length Prefixed (MAX)
        internal readonly byte Precision;   // maximum precision for numeric types
        internal readonly byte Scale;
        internal readonly byte TDSType;
        internal readonly byte NullableType;

        internal readonly string TypeName;    // string name of this type
        internal readonly SqlDbType2 SqlDbType;
        internal readonly DbType DbType;

        //  holds count of property bytes expected for a SQLVariant structure
        internal readonly byte PropBytes;


        // pre-computed fields
        internal readonly bool IsAnsiType;
        internal readonly bool IsBinType;
        internal readonly bool IsCharType;
        internal readonly bool IsNCharType;
        internal readonly bool IsSizeInCharacters;
        internal readonly bool Is2008Type;
        internal readonly bool IsVarTime;

        internal readonly bool Is70Supported;
        internal readonly bool Is80Supported;
        internal readonly bool Is90Supported;
        internal readonly bool Is100Supported;

        public MetaType(byte precision, byte scale, int fixedLength, bool isFixed, bool isLong, bool isPlp, byte tdsType, byte nullableTdsType, string typeName,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
#endif
            Type classType,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
#endif
            Type sqlType, SqlDbType2 sqldbType, DbType dbType, byte propBytes)
        {
            Precision = precision;
            Scale = scale;
            FixedLength = fixedLength;
            IsFixed = isFixed;
            IsLong = isLong;
            IsPlp = isPlp;

            TDSType = tdsType;
            NullableType = nullableTdsType;
            TypeName = typeName;
            SqlDbType = sqldbType;
            DbType = dbType;

            ClassType = classType;
            SqlType = sqlType;
            PropBytes = propBytes;

            IsAnsiType = _IsAnsiType(sqldbType);
            IsBinType = _IsBinType(sqldbType);
            IsCharType = _IsCharType(sqldbType);
            IsNCharType = _IsNCharType(sqldbType);
            IsSizeInCharacters = _IsSizeInCharacters(sqldbType);
            Is2008Type = _Is2008Type(sqldbType);
            IsVarTime = _IsVarTime(sqldbType);

            Is70Supported = _Is70Supported(SqlDbType);
            Is80Supported = _Is80Supported(SqlDbType);
            Is90Supported = _Is90Supported(SqlDbType);
            Is100Supported = _Is100Supported(SqlDbType);
        }

        // properties should be inlined so there should be no perf penalty for using these accessor functions
        public int TypeId => 0;  // partial length prefixed (xml, nvarchar(max),...)

        private static bool _IsAnsiType(SqlDbType2 type) =>
            type == SqlDbType2.Char || type == SqlDbType2.VarChar || type == SqlDbType2.Text;

        // is this type size expressed as count of characters or bytes?
        private static bool _IsSizeInCharacters(SqlDbType2 type) => 
            type == SqlDbType2.NChar ||
            type == SqlDbType2.NVarChar ||
            type == SqlDbType2.Xml ||
            type == SqlDbType2.NText || 
            type == SqlDbType2.Json;

        private static bool _IsCharType(SqlDbType2 type) =>
            type == SqlDbType2.NChar ||
            type == SqlDbType2.NVarChar ||
            type == SqlDbType2.NText ||
            type == SqlDbType2.Char ||
            type == SqlDbType2.VarChar ||
            type == SqlDbType2.Text ||
            type == SqlDbType2.Xml ||
            type == SqlDbType2.Json;

        private static bool _IsNCharType(SqlDbType2 type) =>
            type == SqlDbType2.NChar ||
            type == SqlDbType2.NVarChar ||
            type == SqlDbType2.NText ||
            type == SqlDbType2.Xml ||
            type == SqlDbType2.Json;

        private static bool _IsBinType(SqlDbType2 type) =>
            type == SqlDbType2.Image ||
            type == SqlDbType2.Binary ||
            type == SqlDbType2.VarBinary ||
            type == SqlDbType2.Timestamp ||
            type == SqlDbType2.Udt ||
            (int)type == 24 /*SqlSmallVarBinary*/;

        private static bool _Is70Supported(SqlDbType2 type) =>
            type != SqlDbType2.BigInt &&
            (int)type > 0 &&
            (int)type <= (int)SqlDbType2.VarChar;

        private static bool _Is80Supported(SqlDbType2 type) =>
            (int)type >= 0 &&
            (int)type <= (int)SqlDbType2.Variant;

        private static bool _Is90Supported(SqlDbType2 type) =>
            _Is80Supported(type) ||
            SqlDbType2.Xml == type ||
            SqlDbType2.Udt == type;

        private static bool _Is100Supported(SqlDbType2 type) =>
            _Is90Supported(type) ||
            SqlDbType2.Date == type ||
            SqlDbType2.Time == type ||
            SqlDbType2.DateTime2 == type ||
            SqlDbType2.DateTimeOffset == type;

        private static bool _Is2008Type(SqlDbType2 type) => SqlDbType2.Structured == type;

        internal static bool _IsVarTime(SqlDbType2 type) =>
            type == SqlDbType2.Time || type == SqlDbType2.DateTime2 || type == SqlDbType2.DateTimeOffset;

        //
        // map SqlDbType2 to MetaType class
        //
        internal static MetaType GetMetaTypeFromSqlDbType(SqlDbType2 target, bool isMultiValued)
        { // WebData 113289
            switch (target)
            {
                case SqlDbType2.BigInt:
                    return s_metaBigInt;
                case SqlDbType2.Binary:
                    return s_metaBinary;
                case SqlDbType2.Bit:
                    return s_metaBit;
                case SqlDbType2.Char:
                    return s_metaChar;
                case SqlDbType2.DateTime:
                    return s_metaDateTime;
                case SqlDbType2.Decimal:
                    return MetaDecimal;
                case SqlDbType2.Float:
                    return s_metaFloat;
                case SqlDbType2.Image:
                    return MetaImage;
                case SqlDbType2.Int:
                    return s_metaInt;
                case SqlDbType2.Money:
                    return s_metaMoney;
                case SqlDbType2.NChar:
                    return s_metaNChar;
                case SqlDbType2.NText:
                    return MetaNText;
                case SqlDbType2.NVarChar:
                    return MetaNVarChar;
                case SqlDbType2.Real:
                    return s_metaReal;
                case SqlDbType2.UniqueIdentifier:
                    return s_metaUniqueId;
                case SqlDbType2.SmallDateTime:
                    return s_metaSmallDateTime;
                case SqlDbType2.SmallInt:
                    return s_metaSmallInt;
                case SqlDbType2.SmallMoney:
                    return s_metaSmallMoney;
                case SqlDbType2.Text:
                    return MetaText;
                case SqlDbType2.Timestamp:
                    return s_metaTimestamp;
                case SqlDbType2.TinyInt:
                    return s_metaTinyInt;
                case SqlDbType2.VarBinary:
                    return MetaVarBinary;
                case SqlDbType2.VarChar:
                    return s_metaVarChar;
                case SqlDbType2.Variant:
                    return s_metaVariant;
                case (SqlDbType2)TdsEnums.SmallVarBinary:
                    return s_metaSmallVarBinary;
                case SqlDbType2.Xml:
                    return MetaXml;
                case SqlDbType2.Udt:
                    return MetaUdt;
                case SqlDbType2.Structured:
                    if (isMultiValued)
                    {
                        return s_metaTable;
                    }
                    else
                    {
                        return s_metaSUDT;
                    }
                case SqlDbType2.Date:
                    return s_metaDate;
                case SqlDbType2.Time:
                    return MetaTime;
                case SqlDbType2.DateTime2:
                    return s_metaDateTime2;
                case SqlDbType2.DateTimeOffset:
                    return MetaDateTimeOffset;
                case SqlDbType2.Json:
                    return s_MetaJson;
                default:
                    throw SQL.InvalidSqlDbType(target);
            }
        }

        //
        // map DbType to MetaType class
        //
        internal static MetaType GetMetaTypeFromDbType(DbType target)
        {
            // if we can't map it, we need to throw
            return target switch
            {
                DbType.AnsiString => s_metaVarChar,
                DbType.AnsiStringFixedLength => s_metaChar,
                DbType.Binary => MetaVarBinary,
                DbType.Byte => s_metaTinyInt,
                DbType.Boolean => s_metaBit,
                DbType.Currency => s_metaMoney,
                DbType.Date => s_metaDate,
                DbType.DateTime => s_metaDateTime,
                DbType.Decimal => MetaDecimal,
                DbType.Double => s_metaFloat,
                DbType.Guid => s_metaUniqueId,
                DbType.Int16 => s_metaSmallInt,
                DbType.Int32 => s_metaInt,
                DbType.Int64 => s_metaBigInt,
                DbType.Object => s_metaVariant,
                DbType.Single => s_metaReal,
                DbType.String => MetaNVarChar,
                DbType.StringFixedLength => s_metaNChar,
                DbType.Time => MetaTime,
                DbType.Xml => MetaXml,
                DbType.DateTime2 => s_metaDateTime2,
                DbType.DateTimeOffset => MetaDateTimeOffset,
                // unsupported
                _ => throw ADP.DbTypeNotSupported(target, typeof(SqlDbType)),// no direct mapping, error out
            };
        }

        internal static MetaType GetMaxMetaTypeFromMetaType(MetaType mt)
        {
            // if we can't map it, we need to throw
            return mt.SqlDbType2 switch
            {
                SqlDbType2.VarBinary or SqlDbType2.Binary => MetaMaxVarBinary,
                SqlDbType2.VarChar or SqlDbType2.Char => MetaMaxVarChar,
                SqlDbType2.NVarChar or SqlDbType2.NChar => MetaMaxNVarChar,
                SqlDbType2.Udt => s_metaMaxUdt,
                _ => mt,
            };
        }

        //
        // map COM+ Type to MetaType class
        //
        internal static MetaType GetMetaTypeFromType(Type dataType) => GetMetaTypeFromValue(dataType, null, false, true);

        internal static MetaType GetMetaTypeFromValue(object value, bool streamAllowed = true) => 
            GetMetaTypeFromValue(value.GetType(), value, true, streamAllowed);

        private static MetaType GetMetaTypeFromValue(Type dataType, object value, bool inferLen, bool streamAllowed)
        {
            switch (Type.GetTypeCode(dataType))
            {
                case TypeCode.Empty:
                    throw ADP.InvalidDataType(TypeCode.Empty);
                case TypeCode.Object:
                    if (dataType == typeof(byte[]))
                    {
                        // Must not default to image if inferLen is false
                        // MDAC 90455
                        if (!inferLen || ((byte[])value).Length <= TdsEnums.TYPE_SIZE_LIMIT)
                        {
                            return MetaVarBinary;
                        }
                        else
                        {
                            return MetaImage;
                        }
                    }
                    else if (dataType == typeof(System.Guid))
                    {
                        return s_metaUniqueId;
                    }
                    else if (dataType == typeof(object))
                    {
                        return s_metaVariant;
                    } // check sql types now
                    else if (dataType == typeof(SqlBinary))
                        return MetaVarBinary;
                    else if (dataType == typeof(SqlBoolean))
                        return s_metaBit;
                    else if (dataType == typeof(SqlByte))
                        return s_metaTinyInt;
                    else if (dataType == typeof(SqlBytes))
                        return MetaVarBinary;
                    else if (dataType == typeof(SqlChars))
                        return MetaNVarChar; // MDAC 87587
                    else if (dataType == typeof(SqlDateTime))
                        return s_metaDateTime;
                    else if (dataType == typeof(SqlDouble))
                        return s_metaFloat;
                    else if (dataType == typeof(SqlGuid))
                        return s_metaUniqueId;
                    else if (dataType == typeof(SqlInt16))
                        return s_metaSmallInt;
                    else if (dataType == typeof(SqlInt32))
                        return s_metaInt;
                    else if (dataType == typeof(SqlInt64))
                        return s_metaBigInt;
                    else if (dataType == typeof(SqlMoney))
                        return s_metaMoney;
                    else if (dataType == typeof(SqlDecimal))
                        return MetaDecimal;
                    else if (dataType == typeof(SqlSingle))
                        return s_metaReal;
                    else if (dataType == typeof(SqlXml))
                        return MetaXml;
                    else if (dataType == typeof(SqlString))
                    {
                        return ((inferLen && !((SqlString)value).IsNull)
                            ? PromoteStringType(((SqlString)value).Value)
                            : MetaNVarChar); // MDAC 87587
                    }
                    else if (dataType == typeof(IEnumerable<DbDataRecord>) || dataType == typeof(DataTable))
                    {
                        return s_metaTable;
                    }
                    else if (dataType == typeof(TimeSpan))
                    {
                        return MetaTime;
                    }
                    else if (dataType == typeof(DateTimeOffset))
                    {
                        return MetaDateTimeOffset;
                    }
#if NET6_0_OR_GREATER
                    else if (dataType == typeof(DateOnly))
                    {
                        return s_metaDate;
                    }
                    else if (dataType == typeof(TimeOnly))
                    {
                        return MetaTime;
                    }
#endif
                    else
                    {
                        // UDT ?
                        SqlUdtInfo attribs = SqlUdtInfo.TryGetFromType(dataType);
                        if (attribs != null)
                        {
                            return MetaUdt;
                        }
                        if (streamAllowed)
                        {
                            // Derived from Stream ?
                            if (typeof(Stream).IsAssignableFrom(dataType))
                            {
                                return MetaVarBinary;
                            }
                            // Derived from TextReader ?
                            else if (typeof(TextReader).IsAssignableFrom(dataType))
                            {
                                return MetaNVarChar;
                            }
                            // Derived from XmlReader ?
                            else if (typeof(System.Xml.XmlReader).IsAssignableFrom(dataType))
                            {
                                return MetaXml;
                            }
                        }
                    }
                    throw ADP.UnknownDataType(dataType);

                case TypeCode.DBNull:
                    throw ADP.InvalidDataType(TypeCode.DBNull);
                case TypeCode.Boolean:
                    return s_metaBit;
                case TypeCode.Char:
                    throw ADP.InvalidDataType(TypeCode.Char);
                case TypeCode.SByte:
                    throw ADP.InvalidDataType(TypeCode.SByte);
                case TypeCode.Byte:
                    return s_metaTinyInt;
                case TypeCode.Int16:
                    return s_metaSmallInt;
                case TypeCode.UInt16:
                    throw ADP.InvalidDataType(TypeCode.UInt16);
                case TypeCode.Int32:
                    return s_metaInt;
                case TypeCode.UInt32:
                    throw ADP.InvalidDataType(TypeCode.UInt32);
                case TypeCode.Int64:
                    return s_metaBigInt;
                case TypeCode.UInt64:
                    throw ADP.InvalidDataType(TypeCode.UInt64);
                case TypeCode.Single:
                    return s_metaReal;
                case TypeCode.Double:
                    return s_metaFloat;
                case TypeCode.Decimal:
                    return MetaDecimal;
                case TypeCode.DateTime:
                    return s_metaDateTime;
                case TypeCode.String:
                    return (inferLen ? PromoteStringType((string)value) : MetaNVarChar);
                default:
                    throw ADP.UnknownDataTypeCode(dataType, Type.GetTypeCode(dataType));
            }
        }

        internal static object GetNullSqlValue(Type sqlType)
        {
            if (sqlType == typeof(SqlSingle))
                return SqlSingle.Null;
            else if (sqlType == typeof(SqlString))
                return SqlString.Null;
            else if (sqlType == typeof(SqlDouble))
                return SqlDouble.Null;
            else if (sqlType == typeof(SqlBinary))
                return SqlBinary.Null;
            else if (sqlType == typeof(SqlGuid))
                return SqlGuid.Null;
            else if (sqlType == typeof(SqlBoolean))
                return SqlBoolean.Null;
            else if (sqlType == typeof(SqlByte))
                return SqlByte.Null;
            else if (sqlType == typeof(SqlInt16))
                return SqlInt16.Null;
            else if (sqlType == typeof(SqlInt32))
                return SqlInt32.Null;
            else if (sqlType == typeof(SqlInt64))
                return SqlInt64.Null;
            else if (sqlType == typeof(SqlDecimal))
                return SqlDecimal.Null;
            else if (sqlType == typeof(SqlDateTime))
                return SqlDateTime.Null;
            else if (sqlType == typeof(SqlMoney))
                return SqlMoney.Null;
            else if (sqlType == typeof(SqlXml))
                return SqlXml.Null;
            else if (sqlType == typeof(object))
                return DBNull.Value;
            else if (sqlType == typeof(IEnumerable<DbDataRecord>))
                return DBNull.Value;
            else if (sqlType == typeof(DataTable))
                return DBNull.Value;
            else if (sqlType == typeof(DateTime))
                return DBNull.Value;
            else if (sqlType == typeof(TimeSpan))
                return DBNull.Value;
            else if (sqlType == typeof(DateTimeOffset))
                return DBNull.Value;
            else
            {
                Debug.Fail("Unknown SqlType!");
                return DBNull.Value;
            }
        }

        internal static MetaType PromoteStringType(string s)
        {
            int len = s.Length;

            if ((len << 1) > TdsEnums.TYPE_SIZE_LIMIT)
            {
                return s_metaVarChar; // try as var char since we can send a 8K characters
            }
            return MetaNVarChar; // send 4k chars, but send as unicode
        }

        internal static object GetComValueFromSqlVariant(object sqlVal)
        {
            object comVal = null;

            if (ADP.IsNull(sqlVal))
                return comVal;

            switch (sqlVal)
            {
                case SqlSingle:
                    comVal = ((SqlSingle)sqlVal).Value;
                    break;
                case SqlString:
                    comVal = ((SqlString)sqlVal).Value;
                    break;
                case SqlDouble:
                    comVal = ((SqlDouble)sqlVal).Value;
                    break;
                case SqlBinary:
                    comVal = ((SqlBinary)sqlVal).Value;
                    break;
                case SqlGuid:
                    comVal = ((SqlGuid)sqlVal).Value;
                    break;
                case SqlBoolean:
                    comVal = ((SqlBoolean)sqlVal).Value;
                    break;
                case SqlByte:
                    comVal = ((SqlByte)sqlVal).Value;
                    break;
                case SqlInt16:
                    comVal = ((SqlInt16)sqlVal).Value;
                    break;
                case SqlInt32:
                    comVal = ((SqlInt32)sqlVal).Value;
                    break;
                case SqlInt64:
                    comVal = ((SqlInt64)sqlVal).Value;
                    break;
                case SqlDecimal:
                    comVal = ((SqlDecimal)sqlVal).Value;
                    break;
                case SqlDateTime:
                    comVal = ((SqlDateTime)sqlVal).Value;
                    break;
                case SqlMoney:
                    comVal = ((SqlMoney)sqlVal).Value;
                    break;
                case SqlXml:
                    comVal = ((SqlXml)sqlVal).Value;
                    break;
                default:
                    AssertIsUserDefinedTypeInstance(sqlVal, "unknown SqlType class stored in sqlVal");
                    break;
            }

            return comVal;
        }

        /// <summary>
        /// Assert that the supplied object is an instance of a SQL User-Defined Type (UDT).
        /// </summary>
        /// <param name="sqlValue">Object instance to be tested.</param>
        /// <param name="failedAssertMessage"></param>
        /// <remarks>
        /// This method is only compiled with debug builds, and it a helper method for the GetComValueFromSqlVariant method defined in this class.
        ///
        /// The presence of the SqlUserDefinedTypeAttribute on the object's type
        /// is used to determine if the object is a UDT instance (if present it is a UDT, else it is not).
        /// </remarks>
        /// <exception cref="NullReferenceException">
        /// If sqlValue is null.  Callers must ensure the object is non-null.
        /// </exception>
        [Conditional("DEBUG")]
        private static void AssertIsUserDefinedTypeInstance(object sqlValue, string failedAssertMessage)
        {
            Type type = sqlValue.GetType();
            SqlUserDefinedTypeAttribute[] attributes = (SqlUserDefinedTypeAttribute[])type.GetCustomAttributes(typeof(SqlUserDefinedTypeAttribute), true);

            Debug.Assert(attributes.Length > 0, failedAssertMessage);
        }

        // devnote: This method should not be used with SqlDbType2.Date and SqlDbType2.DateTime2. 
        //          With these types the values should be used directly as CLR types instead of being converted to a SqlValue
        internal static object GetSqlValueFromComVariant(object comVal)
        {
            object sqlVal = null;
            if ((null != comVal) && (DBNull.Value != comVal))
            {
                switch (comVal)
                {
                    case float:
                        sqlVal = new SqlSingle((float)comVal);
                        break;
                    case string:
                        sqlVal = new SqlString((string)comVal);
                        break;
                    case double:
                        sqlVal = new SqlDouble((double)comVal);
                        break;
                    case byte[]:
                        sqlVal = new SqlBinary((byte[])comVal);
                        break;
                    case char:
                        sqlVal = new SqlString(((char)comVal).ToString());
                        break;
                    case char[]:
                        sqlVal = new SqlChars((char[])comVal);
                        break;
                    case System.Guid:
                        sqlVal = new SqlGuid((Guid)comVal);
                        break;
                    case bool:
                        sqlVal = new SqlBoolean((bool)comVal);
                        break;
                    case byte:
                        sqlVal = new SqlByte((byte)comVal);
                        break;
                    case short:
                        sqlVal = new SqlInt16((short)comVal);
                        break;
                    case int:
                        sqlVal = new SqlInt32((int)comVal);
                        break;
                    case long:
                        sqlVal = new SqlInt64((long)comVal);
                        break;
                    case decimal:
                        sqlVal = new SqlDecimal((decimal)comVal);
                        break;
                    case DateTime:
                        // devnote: Do not use with SqlDbType2.Date and SqlDbType2.DateTime2. See comment at top of method.
                        sqlVal = new SqlDateTime((DateTime)comVal);
                        break;
                    case XmlReader:
                        sqlVal = new SqlXml((XmlReader)comVal);
                        break;
                    case TimeSpan:
                    case DateTimeOffset:
#if NET6_0_OR_GREATER
                    case TimeOnly:
                    case DateOnly:
#endif
                        sqlVal = comVal;
                        break;
                    default:
#if DEBUG
                        Debug.Fail("unknown SqlType class stored in sqlVal");
#endif
                        break;
                }
            }
            return sqlVal;
        }

        internal static SqlDbType2 GetSqlDbTypeFromOleDbType(short dbType, string typeName)
        {
#if NETFRAMEWORK
            SqlDbType2 sqlType = SqlDbType2.Variant;
            switch ((OleDbType)dbType)
            {
                case OleDbType.BigInt:
                    sqlType = SqlDbType2.BigInt;
                    break;
                case OleDbType.Boolean:
                    sqlType = SqlDbType2.Bit;
                    break;
                case OleDbType.Char:
                case OleDbType.VarChar:
                    // these guys are ambiguous - server sends over DBTYPE_STR in both cases
                    sqlType = (typeName == MetaTypeName.CHAR) ? SqlDbType2.Char : SqlDbType2.VarChar;
                    break;
                case OleDbType.Currency:
                    sqlType = (typeName == MetaTypeName.SMALLMONEY) ? SqlDbType2.SmallMoney : SqlDbType2.Money;
                    break;
                case OleDbType.Date:
                case OleDbType.DBTimeStamp:
                case OleDbType.Filetime:
                    sqlType = typeName switch
                    {
                        MetaTypeName.SMALLDATETIME => SqlDbType2.SmallDateTime,
                        MetaTypeName.DATETIME2 => SqlDbType2.DateTime2,
                        _ => SqlDbType2.DateTime,
                    };
                    break;
                case OleDbType.Decimal:
                case OleDbType.Numeric:
                    sqlType = SqlDbType2.Decimal;
                    break;
                case OleDbType.Double:
                    sqlType = SqlDbType2.Float;
                    break;
                case OleDbType.Guid:
                    sqlType = SqlDbType2.UniqueIdentifier;
                    break;
                case OleDbType.Integer:
                    sqlType = SqlDbType2.Int;
                    break;
                case OleDbType.LongVarBinary:
                    sqlType = SqlDbType2.Image;
                    break;
                case OleDbType.LongVarChar:
                    sqlType = SqlDbType2.Text;
                    break;
                case OleDbType.LongVarWChar:
                    sqlType = SqlDbType2.NText;
                    break;
                case OleDbType.Single:
                    sqlType = SqlDbType2.Real;
                    break;
                case OleDbType.SmallInt:
                case OleDbType.UnsignedSmallInt:
                    sqlType = SqlDbType2.SmallInt;
                    break;
                case OleDbType.TinyInt:
                case OleDbType.UnsignedTinyInt:
                    sqlType = SqlDbType2.TinyInt;
                    break;
                case OleDbType.VarBinary:
                case OleDbType.Binary:
                    sqlType = (typeName == MetaTypeName.BINARY) ? SqlDbType2.Binary : SqlDbType2.VarBinary;
                    break;
                case OleDbType.Variant:
                    sqlType = SqlDbType2.Variant;
                    break;
                case OleDbType.VarWChar:
                case OleDbType.WChar:
                case OleDbType.BSTR:
                    // these guys are ambiguous - server sends over DBTYPE_WSTR in both cases
                    // BSTR is always assumed to be NVARCHAR
                    sqlType = (typeName == MetaTypeName.NCHAR) ? SqlDbType2.NChar : SqlDbType2.NVarChar;
                    break;
                case OleDbType.DBDate: // Date
                    sqlType = SqlDbType2.Date;
                    break;
                case (OleDbType)132: // Udt
                    sqlType = SqlDbType2.Udt;
                    break;
                case (OleDbType)141: // Xml
                    sqlType = SqlDbType2.Xml;
                    break;
                case (OleDbType)145: // Time
                    sqlType = SqlDbType2.Time;
                    break;
                case (OleDbType)146: // DateTimeOffset
                    sqlType = SqlDbType2.DateTimeOffset;
                    break;
                // TODO: Handle Structured types for derive parameters
                default:
                    break; // no direct mapping, just use SqlDbType2.Variant;
            }
            return sqlType;
#else
            // OleDbTypes not supported
            return SqlDbType2.Variant;
#endif // NETFRAMEWORK
        }

        internal static MetaType GetSqlDataType(int tdsType, uint userType, int length)
        {
            switch (tdsType)
            {
                case TdsEnums.SQLMONEYN:
                    return ((4 == length) ? s_metaSmallMoney : s_metaMoney);
                case TdsEnums.SQLDATETIMN:
                    return ((4 == length) ? s_metaSmallDateTime : s_metaDateTime);
                case TdsEnums.SQLINTN:
                    return ((4 <= length) ? ((4 == length) ? s_metaInt : s_metaBigInt) : ((2 == length) ? s_metaSmallInt : s_metaTinyInt));
                case TdsEnums.SQLFLTN:
                    return ((4 == length) ? s_metaReal : s_metaFloat);
                case TdsEnums.SQLTEXT:
                    return MetaText;
                case TdsEnums.SQLVARBINARY:
                    return s_metaSmallVarBinary;
                case TdsEnums.SQLBIGVARBINARY:
                    return MetaVarBinary;

                case TdsEnums.SQLVARCHAR:
                case TdsEnums.SQLBIGVARCHAR:
                    return s_metaVarChar;

                case TdsEnums.SQLBINARY:
                case TdsEnums.SQLBIGBINARY:
                    return ((TdsEnums.SQLTIMESTAMP == userType) ? s_metaTimestamp : s_metaBinary);

                case TdsEnums.SQLIMAGE:
                    return MetaImage;

                case TdsEnums.SQLCHAR:
                case TdsEnums.SQLBIGCHAR:
                    return s_metaChar;

                case TdsEnums.SQLINT1:
                    return s_metaTinyInt;

                case TdsEnums.SQLBIT:
                case TdsEnums.SQLBITN:
                    return s_metaBit;

                case TdsEnums.SQLINT2:
                    return s_metaSmallInt;
                case TdsEnums.SQLINT4:
                    return s_metaInt;
                case TdsEnums.SQLINT8:
                    return s_metaBigInt;
                case TdsEnums.SQLMONEY:
                    return s_metaMoney;
                case TdsEnums.SQLDATETIME:
                    return s_metaDateTime;
                case TdsEnums.SQLFLT8:
                    return s_metaFloat;
                case TdsEnums.SQLFLT4:
                    return s_metaReal;
                case TdsEnums.SQLMONEY4:
                    return s_metaSmallMoney;
                case TdsEnums.SQLDATETIM4:
                    return s_metaSmallDateTime;

                case TdsEnums.SQLDECIMALN:
                case TdsEnums.SQLNUMERICN:
                    return MetaDecimal;

                case TdsEnums.SQLUNIQUEID:
                    return s_metaUniqueId;
                case TdsEnums.SQLNCHAR:
                    return s_metaNChar;
                case TdsEnums.SQLNVARCHAR:
                    return MetaNVarChar;
                case TdsEnums.SQLNTEXT:
                    return MetaNText;
                case TdsEnums.SQLVARIANT:
                    return s_metaVariant;
                case TdsEnums.SQLUDT:
                    return MetaUdt;
                case TdsEnums.SQLXMLTYPE:
                    return MetaXml;
                case TdsEnums.SQLJSON:
                    return s_MetaJson;
                case TdsEnums.SQLTABLE:
                    return s_metaTable;
                case TdsEnums.SQLDATE:
                    return s_metaDate;
                case TdsEnums.SQLTIME:
                    return MetaTime;
                case TdsEnums.SQLDATETIME2:
                    return s_metaDateTime2;
                case TdsEnums.SQLDATETIMEOFFSET:
                    return MetaDateTimeOffset;

                case TdsEnums.SQLVOID:
                default:
                    Debug.Fail("Unknown type " + tdsType.ToString(CultureInfo.InvariantCulture));
                    throw SQL.InvalidSqlDbType((SqlDbType)tdsType);
            }
        }

        internal static MetaType GetDefaultMetaType() => MetaNVarChar;

        // Converts an XmlReader into String
        internal static string GetStringFromXml(XmlReader xmlreader)
        {
            SqlXml sxml = new(xmlreader);
            return sxml.Value;
        }

        private static readonly MetaType s_metaBigInt = new(19, 255, 8, true, false, false, TdsEnums.SQLINT8, TdsEnums.SQLINTN, MetaTypeName.BIGINT, typeof(long), typeof(SqlInt64), SqlDbType2.BigInt, DbType.Int64, 0);

        private static readonly MetaType s_metaFloat = new(15, 255, 8, true, false, false, TdsEnums.SQLFLT8, TdsEnums.SQLFLTN, MetaTypeName.FLOAT, typeof(double), typeof(SqlDouble), SqlDbType2.Float, DbType.Double, 0);

        private static readonly MetaType s_metaReal = new(7, 255, 4, true, false, false, TdsEnums.SQLFLT4, TdsEnums.SQLFLTN, MetaTypeName.REAL, typeof(float), typeof(SqlSingle), SqlDbType2.Real, DbType.Single, 0);

        // MetaBinary has two bytes of properties for binary and varbinary
        // 2 byte maxlen
        private static readonly MetaType s_metaBinary = new(255, 255, -1, false, false, false, TdsEnums.SQLBIGBINARY, TdsEnums.SQLBIGBINARY, MetaTypeName.BINARY, typeof(byte[]), typeof(SqlBinary), SqlDbType2.Binary, DbType.Binary, 2);

        // Syntactic sugar for the user...timestamps are 8-byte fixed length binary columns
        private static readonly MetaType s_metaTimestamp = new(255, 255, -1, false, false, false, TdsEnums.SQLBIGBINARY, TdsEnums.SQLBIGBINARY, MetaTypeName.TIMESTAMP, typeof(byte[]), typeof(SqlBinary), SqlDbType2.Timestamp, DbType.Binary, 2);

        internal static readonly MetaType MetaVarBinary = new(255, 255, -1, false, false, false, TdsEnums.SQLBIGVARBINARY, TdsEnums.SQLBIGVARBINARY, MetaTypeName.VARBINARY, typeof(byte[]), typeof(SqlBinary), SqlDbType2.VarBinary, DbType.Binary, 2);

        internal static readonly MetaType MetaMaxVarBinary = new(255, 255, -1, false, true, true, TdsEnums.SQLBIGVARBINARY, TdsEnums.SQLBIGVARBINARY, MetaTypeName.VARBINARY, typeof(byte[]), typeof(SqlBinary), SqlDbType2.VarBinary, DbType.Binary, 2);

        // We have an internal type for smallvarbinarys stored on TdsEnums. We
        // store on TdsEnums instead of SqlDbType2 because we do not want to expose
        // this type to the user.
        private static readonly MetaType s_metaSmallVarBinary = new(255, 255, -1, false, false, false, TdsEnums.SQLVARBINARY, TdsEnums.SQLBIGBINARY, "", typeof(byte[]), typeof(SqlBinary), TdsEnums.SmallVarBinary, DbType.Binary, 2);

        internal static readonly MetaType MetaImage = new(255, 255, -1, false, true, false, TdsEnums.SQLIMAGE, TdsEnums.SQLIMAGE, MetaTypeName.IMAGE, typeof(byte[]), typeof(SqlBinary), SqlDbType2.Image, DbType.Binary, 0);

        private static readonly MetaType s_metaBit = new(255, 255, 1, true, false, false, TdsEnums.SQLBIT, TdsEnums.SQLBITN, MetaTypeName.BIT, typeof(bool), typeof(SqlBoolean), SqlDbType2.Bit, DbType.Boolean, 0);

        private static readonly MetaType s_metaTinyInt = new(3, 255, 1, true, false, false, TdsEnums.SQLINT1, TdsEnums.SQLINTN, MetaTypeName.TINYINT, typeof(byte), typeof(SqlByte), SqlDbType2.TinyInt, DbType.Byte, 0);

        private static readonly MetaType s_metaSmallInt = new(5, 255, 2, true, false, false, TdsEnums.SQLINT2, TdsEnums.SQLINTN, MetaTypeName.SMALLINT, typeof(short), typeof(SqlInt16), SqlDbType2.SmallInt, DbType.Int16, 0);

        private static readonly MetaType s_metaInt = new(10, 255, 4, true, false, false, TdsEnums.SQLINT4, TdsEnums.SQLINTN, MetaTypeName.INT, typeof(int), typeof(SqlInt32), SqlDbType2.Int, DbType.Int32, 0);

        // MetaVariant has seven bytes of properties for MetaChar and MetaVarChar
        // 5 byte tds collation
        // 2 byte maxlen
        private static readonly MetaType s_metaChar = new(255, 255, -1, false, false, false, TdsEnums.SQLBIGCHAR, TdsEnums.SQLBIGCHAR, MetaTypeName.CHAR, typeof(string), typeof(SqlString), SqlDbType2.Char, DbType.AnsiStringFixedLength, 7);

        private static readonly MetaType s_metaVarChar = new(255, 255, -1, false, false, false, TdsEnums.SQLBIGVARCHAR, TdsEnums.SQLBIGVARCHAR, MetaTypeName.VARCHAR, typeof(string), typeof(SqlString), SqlDbType2.VarChar, DbType.AnsiString, 7);

        internal static readonly MetaType MetaMaxVarChar = new(255, 255, -1, false, true, true, TdsEnums.SQLBIGVARCHAR, TdsEnums.SQLBIGVARCHAR, MetaTypeName.VARCHAR, typeof(string), typeof(SqlString), SqlDbType2.VarChar, DbType.AnsiString, 7);

        internal static readonly MetaType MetaText = new(255, 255, -1, false, true, false, TdsEnums.SQLTEXT, TdsEnums.SQLTEXT, MetaTypeName.TEXT, typeof(string), typeof(SqlString), SqlDbType2.Text, DbType.AnsiString, 0);

        // MetaVariant has seven bytes of properties for MetaNChar and MetaNVarChar
        // 5 byte tds collation
        // 2 byte maxlen
        private static readonly MetaType s_metaNChar = new(255, 255, -1, false, false, false, TdsEnums.SQLNCHAR, TdsEnums.SQLNCHAR, MetaTypeName.NCHAR, typeof(string), typeof(SqlString), SqlDbType2.NChar, DbType.StringFixedLength, 7);

        internal static readonly MetaType MetaNVarChar = new(255, 255, -1, false, false, false, TdsEnums.SQLNVARCHAR, TdsEnums.SQLNVARCHAR, MetaTypeName.NVARCHAR, typeof(string), typeof(SqlString), SqlDbType2.NVarChar, DbType.String, 7);

        internal static readonly MetaType MetaMaxNVarChar = new(255, 255, -1, false, true, true, TdsEnums.SQLNVARCHAR, TdsEnums.SQLNVARCHAR, MetaTypeName.NVARCHAR, typeof(string), typeof(SqlString), SqlDbType2.NVarChar, DbType.String, 7);

        internal static readonly MetaType MetaNText = new(255, 255, -1, false, true, false, TdsEnums.SQLNTEXT, TdsEnums.SQLNTEXT, MetaTypeName.NTEXT, typeof(string), typeof(SqlString), SqlDbType2.NText, DbType.String, 7);

        // MetaVariant has two bytes of properties for numeric/decimal types
        // 1 byte precision
        // 1 byte scale
        internal static readonly MetaType MetaDecimal = new(38, 4, 17, true, false, false, TdsEnums.SQLNUMERICN, TdsEnums.SQLNUMERICN, MetaTypeName.DECIMAL, typeof(decimal), typeof(SqlDecimal), SqlDbType2.Decimal, DbType.Decimal, 2);

        internal static readonly MetaType MetaXml = new(255, 255, -1, false, true, true, TdsEnums.SQLXMLTYPE, TdsEnums.SQLXMLTYPE, MetaTypeName.XML, typeof(string), typeof(SqlXml), SqlDbType2.Xml, DbType.Xml, 0);

        private static readonly MetaType s_metaDateTime = new(23, 3, 8, true, false, false, TdsEnums.SQLDATETIME, TdsEnums.SQLDATETIMN, MetaTypeName.DATETIME, typeof(System.DateTime), typeof(SqlDateTime), SqlDbType2.DateTime, DbType.DateTime, 0);

        private static readonly MetaType s_metaSmallDateTime = new(16, 0, 4, true, false, false, TdsEnums.SQLDATETIM4, TdsEnums.SQLDATETIMN, MetaTypeName.SMALLDATETIME, typeof(System.DateTime), typeof(SqlDateTime), SqlDbType2.SmallDateTime, DbType.DateTime, 0);

        private static readonly MetaType s_metaMoney = new(19, 255, 8, true, false, false, TdsEnums.SQLMONEY, TdsEnums.SQLMONEYN, MetaTypeName.MONEY, typeof(decimal), typeof(SqlMoney), SqlDbType2.Money, DbType.Currency, 0);

        private static readonly MetaType s_metaSmallMoney = new(10, 255, 4, true, false, false, TdsEnums.SQLMONEY4, TdsEnums.SQLMONEYN, MetaTypeName.SMALLMONEY, typeof(decimal), typeof(SqlMoney), SqlDbType2.SmallMoney, DbType.Currency, 0);

        private static readonly MetaType s_metaUniqueId = new(255, 255, 16, true, false, false, TdsEnums.SQLUNIQUEID, TdsEnums.SQLUNIQUEID, MetaTypeName.ROWGUID, typeof(System.Guid), typeof(SqlGuid), SqlDbType2.UniqueIdentifier, DbType.Guid, 0);

        private static readonly MetaType s_metaVariant = new(255, 255, -1, true, false, false, TdsEnums.SQLVARIANT, TdsEnums.SQLVARIANT, MetaTypeName.VARIANT, typeof(object), typeof(object), SqlDbType2.Variant, DbType.Object, 0);

        internal static readonly MetaType MetaUdt = new(255, 255, -1, false, false, true, TdsEnums.SQLUDT, TdsEnums.SQLUDT, MetaTypeName.UDT, typeof(object), typeof(object), SqlDbType2.Udt, DbType.Object, 0);

        private static readonly MetaType s_metaMaxUdt = new(255, 255, -1, false, true, true, TdsEnums.SQLUDT, TdsEnums.SQLUDT, MetaTypeName.UDT, typeof(object), typeof(object), SqlDbType2.Udt, DbType.Object, 0);

        private static readonly MetaType s_metaTable = new(255, 255, -1, false, false, false, TdsEnums.SQLTABLE, TdsEnums.SQLTABLE, MetaTypeName.TABLE, typeof(IEnumerable<DbDataRecord>), typeof(IEnumerable<DbDataRecord>), SqlDbType2.Structured, DbType.Object, 0);

        private static readonly MetaType s_metaSUDT = new(255, 255, -1, false, false, false, TdsEnums.SQLVOID, TdsEnums.SQLVOID, "", typeof(Server.SqlDataRecord), typeof(Server.SqlDataRecord), SqlDbType2.Structured, DbType.Object, 0);

        private static readonly MetaType s_metaDate = new(255, 255, 3, true, false, false, TdsEnums.SQLDATE, TdsEnums.SQLDATE, MetaTypeName.DATE, typeof(System.DateTime), typeof(System.DateTime), SqlDbType2.Date, DbType.Date, 0);

        internal static readonly MetaType MetaTime = new(255, 7, -1, false, false, false, TdsEnums.SQLTIME, TdsEnums.SQLTIME, MetaTypeName.TIME, typeof(System.TimeSpan), typeof(System.TimeSpan), SqlDbType2.Time, DbType.Time, 1);

        private static readonly MetaType s_metaDateTime2 = new(255, 7, -1, false, false, false, TdsEnums.SQLDATETIME2, TdsEnums.SQLDATETIME2, MetaTypeName.DATETIME2, typeof(System.DateTime), typeof(System.DateTime), SqlDbType2.DateTime2, DbType.DateTime2, 1);

        internal static readonly MetaType MetaDateTimeOffset = new(255, 7, -1, false, false, false, TdsEnums.SQLDATETIMEOFFSET, TdsEnums.SQLDATETIMEOFFSET, MetaTypeName.DATETIMEOFFSET, typeof(System.DateTimeOffset), typeof(System.DateTimeOffset), SqlDbType2.DateTimeOffset, DbType.DateTimeOffset, 1);

        internal static readonly MetaType s_MetaJson = new(255, 255, -1, false, true, true, TdsEnums.SQLJSON, TdsEnums.SQLJSON, MetaTypeName.JSON, typeof(string), typeof(SqlJson), SqlDbType2.Json, DbType.String, 0);

        public static TdsDateTime FromDateTime(DateTime dateTime, byte cb)
        {
            SqlDateTime sqlDateTime;
            TdsDateTime tdsDateTime = new();

            Debug.Assert(cb == 8 || cb == 4, "Invalid date time size!");

            if (cb == 8)
            {
                sqlDateTime = new SqlDateTime(dateTime);
                tdsDateTime.time = sqlDateTime.TimeTicks;
            }
            else
            {
                // note that smalldatetime is days & minutes.
                // Adding 30 seconds ensures proper roundup if the seconds are >= 30
                // The AddSeconds function handles eventual carryover
                sqlDateTime = new SqlDateTime(dateTime.AddSeconds(30));
                tdsDateTime.time = sqlDateTime.TimeTicks / SqlDateTime.SQLTicksPerMinute;
            }
            tdsDateTime.days = sqlDateTime.DayTicks;
            return tdsDateTime;
        }


        public static DateTime ToDateTime(int sqlDays, int sqlTime, int length)
        {
            if (length == 4)
            {
                return new SqlDateTime(sqlDays, sqlTime * SqlDateTime.SQLTicksPerMinute).Value;
            }
            else
            {
                Debug.Assert(length == 8, "invalid length for DateTime");
                return new SqlDateTime(sqlDays, sqlTime).Value;
            }
        }

        internal static int GetTimeSizeFromScale(byte scale)
        {
            if (scale <= 2)
                return 3;

            if (scale <= 4)
                return 4;

            return 5;
        }

        //
        // please leave string sorted alphabetically
        // note that these names should only be used in the context of parameters.  We always send over BIG* and nullable types for SQL Server
        //
        private static class MetaTypeName
        {
            public const string BIGINT = "bigint";
            public const string BINARY = "binary";
            public const string BIT = "bit";
            public const string CHAR = "char";
            public const string DATETIME = "datetime";
            public const string DECIMAL = "decimal";
            public const string FLOAT = "float";
            public const string IMAGE = "image";
            public const string INT = "int";
            public const string MONEY = "money";
            public const string NCHAR = "nchar";
            public const string NTEXT = "ntext";
            public const string NVARCHAR = "nvarchar";
            public const string REAL = "real";
            public const string ROWGUID = "uniqueidentifier";
            public const string SMALLDATETIME = "smalldatetime";
            public const string SMALLINT = "smallint";
            public const string SMALLMONEY = "smallmoney";
            public const string TEXT = "text";
            public const string TIMESTAMP = "timestamp";
            public const string TINYINT = "tinyint";
            public const string UDT = "udt";
            public const string VARBINARY = "varbinary";
            public const string VARCHAR = "varchar";
            public const string VARIANT = "sql_variant";
            public const string XML = "xml";
            public const string TABLE = "table";
            public const string DATE = "date";
            public const string TIME = "time";
            public const string DATETIME2 = "datetime2";
            public const string DATETIMEOFFSET = "datetimeoffset";
            public const string JSON = "json";
        }
    }

    //
    // note: it is the client's responsibility to know what size date time he is working with
    //
    internal struct TdsDateTime
    {
        public int days;  // offset in days from 1/1/1900
        //     private UInt32 time;  // if smalldatetime, this is # of minutes since midnight
        // otherwise: # of 1/300th of a second since midnight
        public int time;
    }
}

