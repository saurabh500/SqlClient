// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.Common;

namespace Microsoft.Data.SqlClient.Server
{
    /// <summary>
    /// Utilities for manipulating smi-related metadata.
    ///   Since this class is built on top of SMI, SMI should not have a dependency on this class
    ///  These are all based off of knowing the clr type of the value
    ///  as an ExtendedClrTypeCode enum for rapid access.
    /// </summary>
    internal class MetaDataUtilsSmi
    {
        internal const SqlDbType InvalidSqlDbType = (SqlDbType)(-1);
        internal const long InvalidMaxLength = -2;


        /// <summary>
        ///  Standard type inference map to get SqlDbType when all you know is the value's type (typecode)
        /// This map's index is off by one (add one to typecode locate correct entry) in order 
        /// to support ExtendedSqlDbType2.Invalid
        /// This array is meant to be accessed from InferSqlDbTypeFromTypeCode.
        /// </summary>
        private static readonly SqlDbType[] s_extendedTypeCodeToSqlDbTypeMap = {
            InvalidSqlDbType,               // Invalid extended type code
            SqlDbType2.Bit,                  // System.Boolean
            SqlDbType2.TinyInt,              // System.Byte
            SqlDbType2.NVarChar,             // System.Char
            SqlDbType2.DateTime,             // System.DateTime
            InvalidSqlDbType,               // System.DBNull doesn't have an inferable SqlDbType
            SqlDbType2.Decimal,              // System.Decimal
            SqlDbType2.Float,                // System.Double
            InvalidSqlDbType,               // null reference doesn't have an inferable SqlDbType
            SqlDbType2.SmallInt,             // System.Int16
            SqlDbType2.Int,                  // System.Int32
            SqlDbType2.BigInt,               // System.Int64
            InvalidSqlDbType,               // System.SByte doesn't have an inferable SqlDbType
            SqlDbType2.Real,                 // System.Single
            SqlDbType2.NVarChar,             // System.String
            InvalidSqlDbType,               // System.UInt16 doesn't have an inferable SqlDbType
            InvalidSqlDbType,               // System.UInt32 doesn't have an inferable SqlDbType
            InvalidSqlDbType,               // System.UInt64 doesn't have an inferable SqlDbType
            InvalidSqlDbType,               // System.Object doesn't have an inferable SqlDbType
            SqlDbType2.VarBinary,            // System.ByteArray
            SqlDbType2.NVarChar,             // System.CharArray
            SqlDbType2.UniqueIdentifier,     // System.Guid
            SqlDbType2.VarBinary,            // System.Data.SqlTypes.SqlBinary
            SqlDbType2.Bit,                  // System.Data.SqlTypes.SqlBoolean
            SqlDbType2.TinyInt,              // System.Data.SqlTypes.SqlByte
            SqlDbType2.DateTime,             // System.Data.SqlTypes.SqlDateTime
            SqlDbType2.Float,                // System.Data.SqlTypes.SqlDouble
            SqlDbType2.UniqueIdentifier,     // System.Data.SqlTypes.SqlGuid
            SqlDbType2.SmallInt,             // System.Data.SqlTypes.SqlInt16
            SqlDbType2.Int,                  // System.Data.SqlTypes.SqlInt32
            SqlDbType2.BigInt,               // System.Data.SqlTypes.SqlInt64
            SqlDbType2.Money,                // System.Data.SqlTypes.SqlMoney
            SqlDbType2.Decimal,              // System.Data.SqlTypes.SqlDecimal
            SqlDbType2.Real,                 // System.Data.SqlTypes.SqlSingle
            SqlDbType2.NVarChar,             // System.Data.SqlTypes.SqlString
            SqlDbType2.NVarChar,             // System.Data.SqlTypes.SqlChars
            SqlDbType2.VarBinary,            // System.Data.SqlTypes.SqlBytes
            SqlDbType2.Xml,                  // System.Data.SqlTypes.SqlXml
            SqlDbType2.Structured,           // System.Data.DataTable
            SqlDbType2.Structured,           // System.Collections.IEnumerable, used for TVPs it must return IDataRecord
            SqlDbType2.Structured,           // System.Collections.Generic.IEnumerable<Microsoft.Data.SqlClient.Server.SqlDataRecord>
            SqlDbType2.Time,                 // System.TimeSpan
            SqlDbType2.DateTimeOffset,       // System.DateTimeOffset
#if NET6_0_OR_GREATER
            SqlDbType2.Date,                 // System.DateOnly
            SqlDbType2.Time,                 // System.TimeOnly  
#endif
        };


        /// <summary>
        /// Dictionary to map from clr type object to ExtendedClrTypeCodeMap enum.
        /// This dictionary should only be accessed from DetermineExtendedTypeCode and class ctor for setup.
        /// </summary>
        private static readonly Dictionary<Type, ExtendedClrTypeCode> s_typeToExtendedTypeCodeMap = CreateTypeToExtendedTypeCodeMap();

        private static Dictionary<Type, ExtendedClrTypeCode> CreateTypeToExtendedTypeCodeMap()
        {
#if NET6_0_OR_GREATER
            int Count = 44;
#else
            int Count = 42;
#endif
            // Keep this initialization list in the same order as ExtendedClrTypeCode for ease in validating!
            var dictionary = new Dictionary<Type, ExtendedClrTypeCode>(Count)
            {
                { typeof(bool), ExtendedClrTypeCode.Boolean },
                { typeof(byte), ExtendedClrTypeCode.Byte },
                { typeof(char), ExtendedClrTypeCode.Char },
                { typeof(DateTime), ExtendedClrTypeCode.DateTime },
                { typeof(DBNull), ExtendedClrTypeCode.DBNull },
                { typeof(decimal), ExtendedClrTypeCode.Decimal },
                { typeof(double), ExtendedClrTypeCode.Double },
                // lookup code will handle special-case null-ref, omitting the addition of ExtendedTypeCode.Empty
                { typeof(short), ExtendedClrTypeCode.Int16 },
                { typeof(int), ExtendedClrTypeCode.Int32 },
                { typeof(long), ExtendedClrTypeCode.Int64 },
                { typeof(sbyte), ExtendedClrTypeCode.SByte },
                { typeof(float), ExtendedClrTypeCode.Single },
                { typeof(string), ExtendedClrTypeCode.String },
                { typeof(ushort), ExtendedClrTypeCode.UInt16 },
                { typeof(uint), ExtendedClrTypeCode.UInt32 },
                { typeof(ulong), ExtendedClrTypeCode.UInt64 },
                { typeof(object), ExtendedClrTypeCode.Object },
                { typeof(byte[]), ExtendedClrTypeCode.ByteArray },
                { typeof(char[]), ExtendedClrTypeCode.CharArray },
                { typeof(Guid), ExtendedClrTypeCode.Guid },
                { typeof(SqlBinary), ExtendedClrTypeCode.SqlBinary },
                { typeof(SqlBoolean), ExtendedClrTypeCode.SqlBoolean },
                { typeof(SqlByte), ExtendedClrTypeCode.SqlByte },
                { typeof(SqlDateTime), ExtendedClrTypeCode.SqlDateTime },
                { typeof(SqlDouble), ExtendedClrTypeCode.SqlDouble },
                { typeof(SqlGuid), ExtendedClrTypeCode.SqlGuid },
                { typeof(SqlInt16), ExtendedClrTypeCode.SqlInt16 },
                { typeof(SqlInt32), ExtendedClrTypeCode.SqlInt32 },
                { typeof(SqlInt64), ExtendedClrTypeCode.SqlInt64 },
                { typeof(SqlMoney), ExtendedClrTypeCode.SqlMoney },
                { typeof(SqlDecimal), ExtendedClrTypeCode.SqlDecimal },
                { typeof(SqlSingle), ExtendedClrTypeCode.SqlSingle },
                { typeof(SqlString), ExtendedClrTypeCode.SqlString },
                { typeof(SqlChars), ExtendedClrTypeCode.SqlChars },
                { typeof(SqlBytes), ExtendedClrTypeCode.SqlBytes },
                { typeof(SqlXml), ExtendedClrTypeCode.SqlXml },
                { typeof(DataTable), ExtendedClrTypeCode.DataTable },
                { typeof(DbDataReader), ExtendedClrTypeCode.DbDataReader },
                { typeof(IEnumerable<SqlDataRecord>), ExtendedClrTypeCode.IEnumerableOfSqlDataRecord },
                { typeof(TimeSpan), ExtendedClrTypeCode.TimeSpan },
                { typeof(DateTimeOffset), ExtendedClrTypeCode.DateTimeOffset },
#if NET6_0_OR_GREATER
                { typeof(DateOnly), ExtendedClrTypeCode.DateOnly },
                { typeof(TimeOnly), ExtendedClrTypeCode.TimeOnly },
#endif
            };
            return dictionary;
        }

        internal static bool IsCharOrXmlType(SqlDbType type) => IsUnicodeType(type) ||
                    IsAnsiType(type) ||
                    type == SqlDbType2.Xml;

        internal static bool IsUnicodeType(SqlDbType type)
        {
            return type == SqlDbType2.NChar ||
                    type == SqlDbType2.NVarChar ||
                    type == SqlDbType2.NText;
        }

        internal static bool IsAnsiType(SqlDbType type) => type == SqlDbType2.Char ||
                    type == SqlDbType2.VarChar ||
                    type == SqlDbType2.Text;

        internal static bool IsBinaryType(SqlDbType type) => type == SqlDbType2.Binary ||
                    type == SqlDbType2.VarBinary ||
                    type == SqlDbType2.Image;

        // Does this type use PLP format values?
        internal static bool IsPlpFormat(SmiMetaData metaData) => 
                    metaData.MaxLength == SmiMetaData.UnlimitedMaxLengthIndicator ||
                    metaData.SqlDbType == SqlDbType2.Image ||
                    metaData.SqlDbType == SqlDbType2.NText ||
                    metaData.SqlDbType == SqlDbType2.Text ||
                    metaData.SqlDbType == SqlDbType2.Udt;

        // If we know we're only going to use this object to assign to a specific SqlDbType back end object,
        //  we can save some processing time by only checking for the few valid types that can be assigned to the dbType.
        //  This assumes a switch statement over SqlDbType is faster than getting the ClrTypeCode and iterating over a
        //  series of if statements, or using a hash table. 
        // NOTE: the form of these checks is taking advantage of a feature of the JIT compiler that is supposed to
        //      optimize checks of the form '(xxx.GetType() == typeof( YYY ))'.  The JIT team claimed at one point that
        //      this doesn't even instantiate a Type instance, thus was the fastest method for individual comparisons.
        //      Given that there's a known SqlDbType, thus a minimal number of comparisons, it's likely this is faster
        //      than the other approaches considered (both GetType().GetTypeCode() switch and hash table using Type keys
        //      must instantiate a Type object.  The typecode switch also degenerates into a large if-then-else for
        //      all but the primitive clr types.
        internal static ExtendedClrTypeCode DetermineExtendedTypeCodeForUseWithSqlDbType(
                SqlDbType dbType,
                bool isMultiValued,
                object value,
                Type udtType
#if NETFRAMEWORK
                ,ulong smiVersion
#endif
            )
        {
            ExtendedClrTypeCode extendedCode = ExtendedClrTypeCode.Invalid;

            // fast-track null, which is valid for all types
            if (null == value)
            {
                extendedCode = ExtendedClrTypeCode.Empty;
            }
            else if (DBNull.Value == value)
            {
                extendedCode = ExtendedClrTypeCode.DBNull;
            }
            else
            {
                switch (dbType)
                {
                    case SqlDbType2.BigInt:
                        if (value.GetType() == typeof(long))
                            extendedCode = ExtendedClrTypeCode.Int64;
                        else if (value.GetType() == typeof(SqlInt64))
                            extendedCode = ExtendedClrTypeCode.SqlInt64;
                        break;
                    case SqlDbType2.Binary:
                    case SqlDbType2.VarBinary:
                    case SqlDbType2.Image:
                    case SqlDbType2.Timestamp:
                        if (value.GetType() == typeof(byte[]))
                            extendedCode = ExtendedClrTypeCode.ByteArray;
                        else if (value.GetType() == typeof(SqlBinary))
                            extendedCode = ExtendedClrTypeCode.SqlBinary;
                        else if (value.GetType() == typeof(SqlBytes))
                            extendedCode = ExtendedClrTypeCode.SqlBytes;
                        else if (value.GetType() == typeof(StreamDataFeed))
                            extendedCode = ExtendedClrTypeCode.Stream;
                        break;
                    case SqlDbType2.Bit:
                        if (value.GetType() == typeof(bool))
                            extendedCode = ExtendedClrTypeCode.Boolean;
                        else if (value.GetType() == typeof(SqlBoolean))
                            extendedCode = ExtendedClrTypeCode.SqlBoolean;
                        break;
                    case SqlDbType2.Char:
                    case SqlDbType2.NChar:
                    case SqlDbType2.NText:
                    case SqlDbType2.NVarChar:
                    case SqlDbType2.Text:
                    case SqlDbType2.VarChar:
                        if (value.GetType() == typeof(string))
                            extendedCode = ExtendedClrTypeCode.String;
                        if (value.GetType() == typeof(TextDataFeed))
                            extendedCode = ExtendedClrTypeCode.TextReader;
                        else if (value.GetType() == typeof(SqlString))
                            extendedCode = ExtendedClrTypeCode.SqlString;
                        else if (value.GetType() == typeof(char[]))
                            extendedCode = ExtendedClrTypeCode.CharArray;
                        else if (value.GetType() == typeof(SqlChars))
                            extendedCode = ExtendedClrTypeCode.SqlChars;
                        else if (value.GetType() == typeof(char))
                            extendedCode = ExtendedClrTypeCode.Char;
                        break;
                    case SqlDbType2.Date:
#if NET6_0_OR_GREATER
                        if (value.GetType() == typeof(DateOnly))
                            extendedCode = ExtendedClrTypeCode.DateOnly;
                        else if (value.GetType() == typeof(DateTime))
                            extendedCode = ExtendedClrTypeCode.DateTime;
                        else if (value.GetType() == typeof(SqlDateTime))
                            extendedCode = ExtendedClrTypeCode.SqlDateTime;

                        break;
#endif
                    case SqlDbType2.DateTime2:
#if NETFRAMEWORK
                        if (smiVersion >= SmiContextFactory.Sql2008Version)
                        {
                            goto case SqlDbType2.DateTime;
                        }
                        break;
#endif
                    case SqlDbType2.DateTime:
                    case SqlDbType2.SmallDateTime:
                        if (value.GetType() == typeof(DateTime))
                            extendedCode = ExtendedClrTypeCode.DateTime;
                        else if (value.GetType() == typeof(SqlDateTime))
                            extendedCode = ExtendedClrTypeCode.SqlDateTime;
                        break;
                    case SqlDbType2.Decimal:
                        if (value.GetType() == typeof(decimal))
                            extendedCode = ExtendedClrTypeCode.Decimal;
                        else if (value.GetType() == typeof(SqlDecimal))
                            extendedCode = ExtendedClrTypeCode.SqlDecimal;
                        break;
                    case SqlDbType2.Real:
                        if (value.GetType() == typeof(float))
                            extendedCode = ExtendedClrTypeCode.Single;
                        else if (value.GetType() == typeof(SqlSingle))
                            extendedCode = ExtendedClrTypeCode.SqlSingle;
                        break;
                    case SqlDbType2.Int:
                        if (value.GetType() == typeof(int))
                            extendedCode = ExtendedClrTypeCode.Int32;
                        else if (value.GetType() == typeof(SqlInt32))
                            extendedCode = ExtendedClrTypeCode.SqlInt32;
                        break;
                    case SqlDbType2.Money:
                    case SqlDbType2.SmallMoney:
                        if (value.GetType() == typeof(SqlMoney))
                            extendedCode = ExtendedClrTypeCode.SqlMoney;
                        else if (value.GetType() == typeof(decimal))
                            extendedCode = ExtendedClrTypeCode.Decimal;
                        break;
                    case SqlDbType2.Float:
                        if (value.GetType() == typeof(SqlDouble))
                            extendedCode = ExtendedClrTypeCode.SqlDouble;
                        else if (value.GetType() == typeof(double))
                            extendedCode = ExtendedClrTypeCode.Double;
                        break;
                    case SqlDbType2.UniqueIdentifier:
                        if (value.GetType() == typeof(SqlGuid))
                            extendedCode = ExtendedClrTypeCode.SqlGuid;
                        else if (value.GetType() == typeof(Guid))
                            extendedCode = ExtendedClrTypeCode.Guid;
                        break;
                    case SqlDbType2.SmallInt:
                        if (value.GetType() == typeof(short))
                            extendedCode = ExtendedClrTypeCode.Int16;
                        else if (value.GetType() == typeof(SqlInt16))
                            extendedCode = ExtendedClrTypeCode.SqlInt16;
                        break;
                    case SqlDbType2.TinyInt:
                        if (value.GetType() == typeof(byte))
                            extendedCode = ExtendedClrTypeCode.Byte;
                        else if (value.GetType() == typeof(SqlByte))
                            extendedCode = ExtendedClrTypeCode.SqlByte;
                        break;
                    case SqlDbType2.Variant:
                        // SqlDbType doesn't help us here, call general-purpose function
                        extendedCode = DetermineExtendedTypeCode(value);

                        // Some types aren't allowed for Variants but are for the general-purpose function.  
                        //  Match behavior of other types and return invalid in these cases.
                        if (ExtendedClrTypeCode.SqlXml == extendedCode)
                        {
                            extendedCode = ExtendedClrTypeCode.Invalid;
                        }
                        break;
                    case SqlDbType2.Udt:
                        // Validate UDT type if caller gave us a type to validate against
                        if (null == udtType || value.GetType() == udtType)
                        {
                            extendedCode = ExtendedClrTypeCode.Object;
                        }
                        else
                        {
                            extendedCode = ExtendedClrTypeCode.Invalid;
                        }
                        break;
#if NET6_0_OR_GREATER
                    case SqlDbType2.Time:
                        if (value.GetType() == typeof(TimeOnly))
                            extendedCode = ExtendedClrTypeCode.TimeOnly;
                        else if (value.GetType() == typeof(TimeSpan))
                            extendedCode = ExtendedClrTypeCode.TimeSpan;
                        break;
#else
                    case SqlDbType2.Time:
                        if (value.GetType() == typeof(TimeSpan)
#if NETFRAMEWORK
                        && smiVersion >= SmiContextFactory.Sql2008Version
#endif
                            )
                            extendedCode = ExtendedClrTypeCode.TimeSpan;
                        break;
#endif
                    case SqlDbType2.DateTimeOffset:
                        if (value.GetType() == typeof(DateTimeOffset)
#if NETFRAMEWORK
                        && smiVersion >= SmiContextFactory.Sql2008Version
#endif
                            )
                            extendedCode = ExtendedClrTypeCode.DateTimeOffset;
                        break;
                    case SqlDbType2.Xml:
                        if (value.GetType() == typeof(SqlXml))
                            extendedCode = ExtendedClrTypeCode.SqlXml;
                        if (value.GetType() == typeof(XmlDataFeed))
                            extendedCode = ExtendedClrTypeCode.XmlReader;
                        else if (value.GetType() == typeof(string))
                            extendedCode = ExtendedClrTypeCode.String;
                        break;
                    case SqlDbType2.Structured:
                        if (isMultiValued)
                        {
                            if (value is DataTable)
                            {
                                extendedCode = ExtendedClrTypeCode.DataTable;
                            }
                            else if (value is IEnumerable<SqlDataRecord>)
                            {
                                extendedCode = ExtendedClrTypeCode.IEnumerableOfSqlDataRecord;
                            }
                            else if (value is DbDataReader)
                            {
                                extendedCode = ExtendedClrTypeCode.DbDataReader;
                            }
                        }
                        break;
                    default:
                        // Leave as invalid
                        break;
                }
            }

            return extendedCode;
        }

        // Method to map from Type to ExtendedTypeCode
        internal static ExtendedClrTypeCode DetermineExtendedTypeCodeFromType(Type clrType)
        {
            ExtendedClrTypeCode resultCode;
            return s_typeToExtendedTypeCodeMap.TryGetValue(clrType, out resultCode) ?
                resultCode :
                ExtendedClrTypeCode.Invalid;
        }

        // Returns the ExtendedClrTypeCode that describes the given value
        internal static ExtendedClrTypeCode DetermineExtendedTypeCode(object value)
        {
            return value != null ?
                DetermineExtendedTypeCodeFromType(value.GetType()) :
                ExtendedClrTypeCode.Empty;
        }

        // returns a sqldbtype for the given type code
        internal static SqlDbType InferSqlDbTypeFromTypeCode(ExtendedClrTypeCode typeCode)
        {
            Debug.Assert(typeCode >= ExtendedClrTypeCode.Invalid && typeCode <= ExtendedClrTypeCode.Last, "Someone added a typecode without adding support here!");

            return s_extendedTypeCodeToSqlDbTypeMap[(int)typeCode + 1];
        }

        // Infer SqlDbType from Type in the general case. 2008-only (or later) features that need to 
        //  infer types should use InferSqlDbTypeFromType_2008.
        internal static SqlDbType InferSqlDbTypeFromType(Type type)
        {
            ExtendedClrTypeCode typeCode = DetermineExtendedTypeCodeFromType(type);
            SqlDbType returnType;
            if (ExtendedClrTypeCode.Invalid == typeCode)
            {
                returnType = InvalidSqlDbType;  // Return invalid type so caller can generate specific error
            }
            else
            {
                returnType = InferSqlDbTypeFromTypeCode(typeCode);
            }

            return returnType;
        }

        // Inference rules changed for 2008-or-later-only cases.  Only features that are guaranteed to be 
        //  running against 2008 and don't have backward compat issues should call this code path.
        //      example: TVP's are a new 2008 feature (no back compat issues) so can infer DATETIME2
        //          when mapping System.DateTime from DateTable or DbDataReader.  DATETIME2 is better because
        //          of greater range that can handle all DateTime values.
        internal static SqlDbType InferSqlDbTypeFromType_2008(Type type)
        {
            SqlDbType returnType = InferSqlDbTypeFromType(type);
            if (SqlDbType2.DateTime == returnType)
            {
                returnType = SqlDbType2.DateTime2;
            }
            return returnType;
        }

        /// <summary>
        /// Convert SqlMetaData instance to an SmiExtendedMetaData instance.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        internal static SmiExtendedMetaData SqlMetaDataToSmiExtendedMetaData(SqlMetaData source)
        {
            // now map everything across to the extended metadata object
            string typeSpecificNamePart1 = null;
            string typeSpecificNamePart2 = null;
            string typeSpecificNamePart3 = null;

            if (SqlDbType2.Xml == source.SqlDbType)
            {
                typeSpecificNamePart1 = source.XmlSchemaCollectionDatabase;
                typeSpecificNamePart2 = source.XmlSchemaCollectionOwningSchema;
                typeSpecificNamePart3 = source.XmlSchemaCollectionName;
            }
            else if (SqlDbType2.Udt == source.SqlDbType)
            {
                // Split the input name. UdtTypeName is specified as single 3 part name.
                // NOTE: ParseUdtTypeName throws if format is incorrect
                string typeName = source.ServerTypeName;
                if (null != typeName)
                {
                    string[] names = SqlParameter.ParseTypeName(typeName, true /* isUdtTypeName */);

                    if (1 == names.Length)
                    {
                        typeSpecificNamePart3 = names[0];
                    }
                    else if (2 == names.Length)
                    {
                        typeSpecificNamePart2 = names[0];
                        typeSpecificNamePart3 = names[1];
                    }
                    else if (3 == names.Length)
                    {
                        typeSpecificNamePart1 = names[0];
                        typeSpecificNamePart2 = names[1];
                        typeSpecificNamePart3 = names[2];
                    }
                    else
                    {
                        throw ADP.ArgumentOutOfRange(nameof(typeName));
                    }

                    if ((!string.IsNullOrEmpty(typeSpecificNamePart1) && TdsEnums.MAX_SERVERNAME < typeSpecificNamePart1.Length)
                        || (!string.IsNullOrEmpty(typeSpecificNamePart2) && TdsEnums.MAX_SERVERNAME < typeSpecificNamePart2.Length)
                        || (!string.IsNullOrEmpty(typeSpecificNamePart3) && TdsEnums.MAX_SERVERNAME < typeSpecificNamePart3.Length))
                    {
                        throw ADP.ArgumentOutOfRange(nameof(typeName));
                    }
                }
            }

            return new SmiExtendedMetaData(source.SqlDbType,
                                            source.MaxLength,
                                            source.Precision,
                                            source.Scale,
                                            source.LocaleId,
                                            source.CompareOptions,
                                            source.Type,
                                            source.Name,
                                            typeSpecificNamePart1,
                                            typeSpecificNamePart2,
                                            typeSpecificNamePart3);
        }

        // compare SmiMetaData to SqlMetaData and determine if they are compatible.
        internal static bool IsCompatible(SmiMetaData firstMd, SqlMetaData secondMd)
        {
            return firstMd.SqlDbType == secondMd.SqlDbType &&
                    firstMd.MaxLength == secondMd.MaxLength &&
                    firstMd.Precision == secondMd.Precision &&
                    firstMd.Scale == secondMd.Scale &&
                    firstMd.CompareOptions == secondMd.CompareOptions &&
                    firstMd.LocaleId == secondMd.LocaleId &&
                    firstMd.Type == secondMd.Type &&
                    firstMd.SqlDbType != SqlDbType2.Structured &&  // SqlMetaData doesn't support Structured types
                    !firstMd.IsMultiValued;  // SqlMetaData doesn't have a "multivalued" option
        }

        // Extract metadata for a single DataColumn
        internal static SmiExtendedMetaData SmiMetaDataFromDataColumn(DataColumn column, DataTable parent)
        {
            SqlDbType dbType = InferSqlDbTypeFromType_2008(column.DataType);
            if (InvalidSqlDbType == dbType)
            {
                throw SQL.UnsupportedColumnTypeForSqlProvider(column.ColumnName, column.DataType.Name);
            }

            long maxLength = AdjustMaxLength(dbType, column.MaxLength);
            if (InvalidMaxLength == maxLength)
            {
                throw SQL.InvalidColumnMaxLength(column.ColumnName, maxLength);
            }

            byte precision;
            byte scale;
            if (column.DataType == typeof(SqlDecimal))
            {
                // Must scan all values in column to determine best-fit precision & scale
                Debug.Assert(null != parent);
                scale = 0;
                byte nonFractionalPrecision = 0; // finds largest non-Fractional portion of precision
                foreach (DataRow row in parent.Rows)
                {
                    object obj = row[column];
                    if (!(obj is DBNull))
                    {
                        SqlDecimal value = (SqlDecimal)obj;
                        if (!value.IsNull)
                        {
                            byte tempNonFractPrec = checked((byte)(value.Precision - value.Scale));
                            if (tempNonFractPrec > nonFractionalPrecision)
                            {
                                nonFractionalPrecision = tempNonFractPrec;
                            }

                            if (value.Scale > scale)
                            {
                                scale = value.Scale;
                            }
                        }
                    }
                }

                precision = checked((byte)(nonFractionalPrecision + scale));

                if (SqlDecimal.MaxPrecision < precision)
                {
                    throw SQL.InvalidTableDerivedPrecisionForTvp(column.ColumnName, precision);
                }
                else if (0 == precision)
                {
                    precision = 1;
                }
            }
            else if (dbType == SqlDbType2.DateTime2 || dbType == SqlDbType2.DateTimeOffset || dbType == SqlDbType2.Time)
            {
                // Time types care about scale, too.  But have to infer maximums for these.
                precision = 0;
                scale = SmiMetaData.DefaultTime.Scale;
            }
            else if (dbType == SqlDbType2.Decimal)
            {
                // Must scan all values in column to determine best-fit precision & scale
                Debug.Assert(null != parent);
                scale = 0;
                byte nonFractionalPrecision = 0; // finds largest non-Fractional portion of precision
                foreach (DataRow row in parent.Rows)
                {
                    object obj = row[column];
                    if (!(obj is DBNull))
                    {
                        SqlDecimal value = (SqlDecimal)(decimal)obj;
                        byte tempNonFractPrec = checked((byte)(value.Precision - value.Scale));
                        if (tempNonFractPrec > nonFractionalPrecision)
                        {
                            nonFractionalPrecision = tempNonFractPrec;
                        }

                        if (value.Scale > scale)
                        {
                            scale = value.Scale;
                        }
                    }
                }

                precision = checked((byte)(nonFractionalPrecision + scale));

                if (SqlDecimal.MaxPrecision < precision)
                {
                    throw SQL.InvalidTableDerivedPrecisionForTvp(column.ColumnName, precision);
                }
                else if (0 == precision)
                {
                    precision = 1;
                }
            }
            else
            {
                precision = 0;
                scale = 0;
            }

            // In Net Core, since DataColumn.Locale is not accessible because it is internal and in a separate assembly, 
            // we try to get the Locale from the parent
            CultureInfo columnLocale = ((null != parent) ? parent.Locale : CultureInfo.CurrentCulture);

            return new SmiExtendedMetaData(
                                        dbType,
                                        maxLength,
                                        precision,
                                        scale,
                                        columnLocale.LCID,
                                        SmiMetaData.DefaultNVarChar.CompareOptions,
#if NETFRAMEWORK
                                        column.DataType,
#else
                                        null,
#endif
                                        false,  // no support for multi-valued columns in a TVP yet
                                        null,   // no support for structured columns yet
                                        null,   // no support for structured columns yet
                                        column.ColumnName,
                                        null,
                                        null,
                                        null);
        }

        internal static long AdjustMaxLength(SqlDbType dbType, long maxLength)
        {
            if (SmiMetaData.UnlimitedMaxLengthIndicator != maxLength)
            {
                if (maxLength < 0)
                {
                    maxLength = InvalidMaxLength;
                }

                switch (dbType)
                {
                    case SqlDbType2.Binary:
                        if (maxLength > SmiMetaData.MaxBinaryLength)
                        {
                            maxLength = InvalidMaxLength;
                        }
                        break;
                    case SqlDbType2.Char:
                        if (maxLength > SmiMetaData.MaxANSICharacters)
                        {
                            maxLength = InvalidMaxLength;
                        }
                        break;
                    case SqlDbType2.NChar:
                        if (maxLength > SmiMetaData.MaxUnicodeCharacters)
                        {
                            maxLength = InvalidMaxLength;
                        }
                        break;
                    case SqlDbType2.NVarChar:
                        // Promote to MAX type if it won't fit in a normal type
                        if (SmiMetaData.MaxUnicodeCharacters < maxLength)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                        break;
                    case SqlDbType2.VarBinary:
                        // Promote to MAX type if it won't fit in a normal type
                        if (SmiMetaData.MaxBinaryLength < maxLength)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                        break;
                    case SqlDbType2.VarChar:
                        // Promote to MAX type if it won't fit in a normal type
                        if (SmiMetaData.MaxANSICharacters < maxLength)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                        break;
                    default:
                        break;
                }
            }

            return maxLength;
        }

        // Map SmiMetaData from a schema table.
        // DEVNOTE: Since we're using SchemaTable, we can assume that we aren't directly using a SqlDataReader
        // so we don't support the Sql-specific stuff, like collation.
        internal static SmiExtendedMetaData SmiMetaDataFromSchemaTableRow(DataRow schemaRow)
        {
            string colName = "";
            object temp = schemaRow[SchemaTableColumn.ColumnName];
            if (DBNull.Value != temp)
            {
                colName = (string)temp;
            }

            // Determine correct SqlDbType2.
            temp = schemaRow[SchemaTableColumn.DataType];
            if (DBNull.Value == temp)
            {
                throw SQL.NullSchemaTableDataTypeNotSupported(colName);
            }

            Type colType = (Type)temp;
            SqlDbType colDbType = InferSqlDbTypeFromType_2008(colType);
            if (InvalidSqlDbType == colDbType)
            {
                // Unknown through standard mapping, use VarBinary for columns that are Object typed, otherwise error
                if (typeof(object) == colType)
                {
                    colDbType = SqlDbType2.VarBinary;
                }
                else
                {
                    throw SQL.UnsupportedColumnTypeForSqlProvider(colName, colType.ToString());
                }
            }

            // Determine metadata modifier values per type (maxlength, precision, scale, etc)
            long maxLength = 0;
            byte precision = 0;
            byte scale = 0;
            switch (colDbType)
            {
                case SqlDbType2.BigInt:
                case SqlDbType2.Bit:
                case SqlDbType2.DateTime:
                case SqlDbType2.Float:
                case SqlDbType2.Image:
                case SqlDbType2.Int:
                case SqlDbType2.Money:
                case SqlDbType2.NText:
                case SqlDbType2.Real:
                case SqlDbType2.UniqueIdentifier:
                case SqlDbType2.SmallDateTime:
                case SqlDbType2.SmallInt:
                case SqlDbType2.SmallMoney:
                case SqlDbType2.Text:
                case SqlDbType2.Timestamp:
                case SqlDbType2.TinyInt:
                case SqlDbType2.Variant:
                case SqlDbType2.Xml:
                case SqlDbType2.Date:
                    // These types require no  metadata modifies
                    break;
                case SqlDbType2.Binary:
                case SqlDbType2.VarBinary:
                    // These types need a binary max length
                    temp = schemaRow[SchemaTableColumn.ColumnSize];
                    if (DBNull.Value == temp)
                    {
                        // source isn't specifying a size, so assume the worst
                        if (SqlDbType2.Binary == colDbType)
                        {
                            maxLength = SmiMetaData.MaxBinaryLength;
                        }
                        else
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                    }
                    else
                    {
                        // We (should) have a valid maxlength, so use it.
                        maxLength = Convert.ToInt64(temp, null);

                        // Max length must be 0 to MaxBinaryLength or it can be UnlimitedMAX if type is varbinary.
                        // If it's greater than MaxBinaryLength, just promote it to UnlimitedMAX, if possible.
                        if (maxLength > SmiMetaData.MaxBinaryLength)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }

                        if ((maxLength < 0 &&
                                (maxLength != SmiMetaData.UnlimitedMaxLengthIndicator ||
                                 SqlDbType2.Binary == colDbType)))
                        {
                            throw SQL.InvalidColumnMaxLength(colName, maxLength);
                        }
                    }
                    break;
                case SqlDbType2.Char:
                case SqlDbType2.VarChar:
                    // These types need an ANSI max length
                    temp = schemaRow[SchemaTableColumn.ColumnSize];
                    if (DBNull.Value == temp)
                    {
                        // source isn't specifying a size, so assume the worst
                        if (SqlDbType2.Char == colDbType)
                        {
                            maxLength = SmiMetaData.MaxANSICharacters;
                        }
                        else
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                    }
                    else
                    {
                        // We (should) have a valid maxlength, so use it.
                        maxLength = Convert.ToInt64(temp, null);

                        // Max length must be 0 to MaxANSICharacters or it can be UnlimitedMAX if type is varbinary.
                        // If it's greater than MaxANSICharacters, just promote it to UnlimitedMAX, if possible.
                        if (maxLength > SmiMetaData.MaxANSICharacters)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }

                        if ((maxLength < 0 &&
                                (maxLength != SmiMetaData.UnlimitedMaxLengthIndicator ||
                                 SqlDbType2.Char == colDbType)))
                        {
                            throw SQL.InvalidColumnMaxLength(colName, maxLength);
                        }
                    }
                    break;
                case SqlDbType2.NChar:
                case SqlDbType2.NVarChar:
                    // These types need a unicode max length
                    temp = schemaRow[SchemaTableColumn.ColumnSize];
                    if (DBNull.Value == temp)
                    {
                        // source isn't specifying a size, so assume the worst
                        if (SqlDbType2.NChar == colDbType)
                        {
                            maxLength = SmiMetaData.MaxUnicodeCharacters;
                        }
                        else
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }
                    }
                    else
                    {
                        // We (should) have a valid maxlength, so use it.
                        maxLength = Convert.ToInt64(temp, null);

                        // Max length must be 0 to MaxUnicodeCharacters or it can be UnlimitedMAX if type is varbinary.
                        // If it's greater than MaxUnicodeCharacters, just promote it to UnlimitedMAX, if possible.
                        if (maxLength > SmiMetaData.MaxUnicodeCharacters)
                        {
                            maxLength = SmiMetaData.UnlimitedMaxLengthIndicator;
                        }

                        if ((maxLength < 0 &&
                                (maxLength != SmiMetaData.UnlimitedMaxLengthIndicator ||
                                 SqlDbType2.NChar == colDbType)))
                        {
                            throw SQL.InvalidColumnMaxLength(colName, maxLength);
                        }
                    }
                    break;
                case SqlDbType2.Decimal:
                    // Decimal requires precision and scale
                    temp = schemaRow[SchemaTableColumn.NumericPrecision];
                    if (DBNull.Value == temp)
                    {
                        precision = SmiMetaData.DefaultDecimal.Precision;
                    }
                    else
                    {
                        precision = Convert.ToByte(temp, null);
                    }

                    temp = schemaRow[SchemaTableColumn.NumericScale];
                    if (DBNull.Value == temp)
                    {
                        scale = SmiMetaData.DefaultDecimal.Scale;
                    }
                    else
                    {
                        scale = Convert.ToByte(temp, null);
                    }

                    if (precision < SmiMetaData.MinPrecision ||
                            precision > SqlDecimal.MaxPrecision ||
                            scale < SmiMetaData.MinScale ||
                            scale > SqlDecimal.MaxScale ||
                            scale > precision)
                    {
                        throw SQL.InvalidColumnPrecScale();
                    }
                    break;
                case SqlDbType2.Time:
                case SqlDbType2.DateTime2:
                case SqlDbType2.DateTimeOffset:
                    // requires scale
                    temp = schemaRow[SchemaTableColumn.NumericScale];
                    if (DBNull.Value == temp)
                    {
                        scale = SmiMetaData.DefaultTime.Scale;
                    }
                    else
                    {
                        scale = Convert.ToByte(temp, null);
                    }

                    if (scale > SmiMetaData.MaxTimeScale)
                    {
                        throw SQL.InvalidColumnPrecScale();
                    }
                    else if (scale < 0)
                    {
                        scale = SmiMetaData.DefaultTime.Scale;
                    }
                    break;
                case SqlDbType2.Udt:
                case SqlDbType2.Structured:
                default:
                    // These types are not supported from SchemaTable
                    throw SQL.UnsupportedColumnTypeForSqlProvider(colName, colType.ToString());
            }

            return new SmiExtendedMetaData(
                            colDbType,
                            maxLength,
                            precision,
                            scale,
                            System.Globalization.CultureInfo.CurrentCulture.LCID,
                            SmiMetaData.GetDefaultForType(colDbType).CompareOptions,
                            null,
                            false,  // no support for multi-valued columns in a TVP yet
                            null,   // no support for structured columns yet
                            null,   // no support for structured columns yet
                            colName,
                            null,
                            null,
                            null);
        }

#if NETFRAMEWORK

        static internal bool IsValidForSmiVersion(SmiExtendedMetaData md, ulong smiVersion)
        {
            if (SmiContextFactory.LatestVersion == smiVersion)
            {
                return true;
            }
            else
            {
                // 2005 doesn't support Structured nor the new time types
                Debug.Assert(SmiContextFactory.Sql2005Version == smiVersion, "Other versions should have been eliminated during link stage");
                return md.SqlDbType != SqlDbType2.Structured &&
                        md.SqlDbType != SqlDbType2.Date &&
                        md.SqlDbType != SqlDbType2.DateTime2 &&
                        md.SqlDbType != SqlDbType2.DateTimeOffset &&
                        md.SqlDbType != SqlDbType2.Time;
            }
        }

#endif
    }
}
