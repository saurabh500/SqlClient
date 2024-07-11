// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Data;
using System.Globalization;
using System.Data.SqlTypes;
using Microsoft.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Microsoft.Data.SqlClient.Server
{
    /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/SqlMetaData/*' />
    // class SqlMetaData
    //   Simple immutable implementation of the a metadata-holding class.  Only
    //    complexities are:
    //        1) enforcing immutability.
    //        2) Inferring type from a value.
    //        3) Adjusting a value to match the metadata.
    public sealed partial class SqlMetaData
    {
        private const long MaxUnicodeLength = 4000;
        private const long MaxANSILength = 8000;
        private const long MaxBinaryLength = 8000;
        private const long UnlimitedMaxLength = -1; // unlimited (except by implementation) max-length.
        private const bool DefaultUseServerDefault = false;
        private const bool DefaultIsUniqueKey = false;
        private const SortOrder DefaultColumnSortOrder = SortOrder.Unspecified;
        private const int DefaultSortOrdinal = -1;
        private const byte MaxTimeScale = 7;

        private const SqlCompareOptions DefaultStringCompareOptions = SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth;

        private static readonly SqlMoney s_smallMoneyMax = new SqlMoney(((decimal)int.MaxValue) / 10000);
        private static readonly SqlMoney s_smallMoneyMin = new SqlMoney(((decimal)int.MinValue) / 10000);

        private static readonly DateTime s_smallDateTimeMax = new DateTime(2079, 06, 06, 23, 59, 29, 998);
        private static readonly DateTime s_smallDateTimeMin = new DateTime(1899, 12, 31, 23, 59, 29, 999);

        private static readonly TimeSpan s_timeMin = TimeSpan.Zero;
        private static readonly TimeSpan s_timeMax = new TimeSpan(TimeSpan.TicksPerDay - 1);

        private static readonly byte[] s_maxLenFromPrecision = new byte[] {5,5,5,5,5,5,5,5,5,9,9,9,9,9,
        9,9,9,9,9,13,13,13,13,13,13,13,13,13,17,17,17,17,17,17,17,17,17,17};

        private static readonly byte[] s_maxVarTimeLenOffsetFromScale = new byte[] { 2, 2, 2, 1, 1, 0, 0, 0 };

        private static readonly long[] s_unitTicksFromScale = {
            10000000,
            1000000,
            100000,
            10000,
            1000,
            100,
            10,
            1,
        };

        private static readonly DbType[] s_sqlDbTypeToDbType = {
            DbType.Int64,           // SqlDbType2.BigInt
            DbType.Binary,          // SqlDbType2.Binary
            DbType.Boolean,         // SqlDbType2.Bit
            DbType.AnsiString,      // SqlDbType2.Char
            DbType.DateTime,        // SqlDbType2.DateTime
            DbType.Decimal,         // SqlDbType2.Decimal
            DbType.Double,          // SqlDbType2.Float
            DbType.Binary,          // SqlDbType2.Image
            DbType.Int32,           // SqlDbType2.Int
            DbType.Currency,        // SqlDbType2.Money
            DbType.String,          // SqlDbType2.NChar
            DbType.String,          // SqlDbType2.NText
            DbType.String,          // SqlDbType2.NVarChar
            DbType.Single,          // SqlDbType2.Real
            DbType.Guid,            // SqlDbType2.UniqueIdentifier
            DbType.DateTime,        // SqlDbType2.SmallDateTime
            DbType.Int16,           // SqlDbType2.SmallInt
            DbType.Currency,        // SqlDbType2.SmallMoney
            DbType.AnsiString,      // SqlDbType2.Text
            DbType.Binary,          // SqlDbType2.Timestamp
            DbType.Byte,            // SqlDbType2.TinyInt
            DbType.Binary,          // SqlDbType2.VarBinary
            DbType.AnsiString,      // SqlDbType2.VarChar
            DbType.Object,          // SqlDbType2.Variant
            DbType.Object,          // SqlDbType2.Row
            DbType.Xml,             // SqlDbType2.Xml
            DbType.String,          // SqlDbType2.NVarChar, place holder
            DbType.String,          // SqlDbType2.NVarChar, place holder
            DbType.String,          // SqlDbType2.NVarChar, place holder
            DbType.Object,          // SqlDbType2.Udt
            DbType.Object,          // SqlDbType2.Structured
            DbType.Date,            // SqlDbType2.Date
            DbType.Time,            // SqlDbType2.Time
            DbType.DateTime2,       // SqlDbType2.DateTime2
            DbType.DateTimeOffset   // SqlDbType2.DateTimeOffset
        };


        // Array of default-valued metadata ordered by corresponding SqlDbType2.
        internal static SqlMetaData[] s_defaults =
            {
            //    new SqlMetaData(name, DbType, SqlDbType, MaxLen, Prec, Scale, Locale, DatabaseName, SchemaName, isPartialLength)
            new SqlMetaData("bigint", SqlDbType2.BigInt,
                    8, 19, 0, 0, SqlCompareOptions.None,  false),            // SqlDbType2.BigInt
            new SqlMetaData("binary", SqlDbType2.Binary,
                    1, 0, 0, 0, SqlCompareOptions.None,  false),                // SqlDbType2.Binary
            new SqlMetaData("bit", SqlDbType2.Bit,
                    1, 1, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.Bit
            new SqlMetaData("char", SqlDbType2.Char,
                    1, 0, 0, 0, DefaultStringCompareOptions,  false),                // SqlDbType2.Char
            new SqlMetaData("datetime", SqlDbType2.DateTime,
                    8, 23, 3, 0, SqlCompareOptions.None, false),            // SqlDbType2.DateTime
            new SqlMetaData("decimal", SqlDbType2.Decimal,
                    9, 18, 0, 0, SqlCompareOptions.None,  false),            // SqlDbType2.Decimal
            new SqlMetaData("float", SqlDbType2.Float,
                    8, 53, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.Float
            new SqlMetaData("image", SqlDbType2.Image,
                    UnlimitedMaxLength, 0, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.Image
            new SqlMetaData("int", SqlDbType2.Int,
                    4, 10, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.Int
            new SqlMetaData("money", SqlDbType2.Money,
                    8, 19, 4, 0, SqlCompareOptions.None, false),            // SqlDbType2.Money
            new SqlMetaData("nchar", SqlDbType2.NChar,
                    1, 0, 0, 0, DefaultStringCompareOptions, false),                // SqlDbType2.NChar
            new SqlMetaData("ntext", SqlDbType2.NText,
                    UnlimitedMaxLength, 0, 0, 0, DefaultStringCompareOptions, false),                // SqlDbType2.NText
            new SqlMetaData("nvarchar", SqlDbType2.NVarChar,
                    MaxUnicodeLength, 0, 0, 0, DefaultStringCompareOptions, false),                // SqlDbType2.NVarChar
            new SqlMetaData("real", SqlDbType2.Real,
                    4, 24, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.Real
            new SqlMetaData("uniqueidentifier", SqlDbType2.UniqueIdentifier,
                    16, 0, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.UniqueIdentifier
            new SqlMetaData("smalldatetime", SqlDbType2.SmallDateTime,
                    4, 16, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.SmallDateTime
            new SqlMetaData("smallint", SqlDbType2.SmallInt,
                    2, 5, 0, 0, SqlCompareOptions.None, false),                                    // SqlDbType2.SmallInt
            new SqlMetaData("smallmoney", SqlDbType2.SmallMoney,
                    4, 10, 4, 0, SqlCompareOptions.None, false),                // SqlDbType2.SmallMoney
            new SqlMetaData("text", SqlDbType2.Text,
                    UnlimitedMaxLength, 0, 0, 0, DefaultStringCompareOptions, false),                // SqlDbType2.Text
            new SqlMetaData("timestamp", SqlDbType2.Timestamp,
                    8, 0, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.Timestamp
            new SqlMetaData("tinyint", SqlDbType2.TinyInt,
                    1, 3, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.TinyInt
            new SqlMetaData("varbinary", SqlDbType2.VarBinary,
                    MaxBinaryLength, 0, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.VarBinary
            new SqlMetaData("varchar", SqlDbType2.VarChar,
                    MaxANSILength, 0, 0, 0, DefaultStringCompareOptions, false),                // SqlDbType2.VarChar
            new SqlMetaData("sql_variant", SqlDbType2.Variant,
                    8016, 0, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.Variant
            new SqlMetaData("nvarchar", SqlDbType2.NVarChar,
                    1, 0, 0, 0, DefaultStringCompareOptions, false),                // Placeholder for value 24
            new SqlMetaData("xml", SqlDbType2.Xml,
                    UnlimitedMaxLength, 0, 0, 0, DefaultStringCompareOptions, true),                // SqlDbType2.Xml
            new SqlMetaData("nvarchar", SqlDbType2.NVarChar,
                    1, 0, 0, 0, DefaultStringCompareOptions, false),                // Placeholder for value 26
            new SqlMetaData("nvarchar", SqlDbType2.NVarChar,
                    MaxUnicodeLength, 0, 0, 0, DefaultStringCompareOptions, false),                // Placeholder for value 27
            new SqlMetaData("nvarchar", SqlDbType2.NVarChar,
                    MaxUnicodeLength, 0, 0, 0, DefaultStringCompareOptions, false),                // Placeholder for value 28
            new SqlMetaData("udt", SqlDbType2.Udt,
                    0, 0, 0, 0, SqlCompareOptions.None, false),            // SqlDbType2.Udt = 29
            new SqlMetaData("table", SqlDbType2.Structured,
                    0, 0, 0, 0, SqlCompareOptions.None, false),                // SqlDbType2.Structured
            new SqlMetaData("date", SqlDbType2.Date,
                    3, 10,0, 0, SqlCompareOptions.None, false),                // SqlDbType2.Date
            new SqlMetaData("time", SqlDbType2.Time,
                    5, 0, 7, 0, SqlCompareOptions.None, false),                // SqlDbType2.Time
            new SqlMetaData("datetime2", SqlDbType2.DateTime2,
                    8, 0, 7, 0, SqlCompareOptions.None, false),                // SqlDbType2.DateTime2
            new SqlMetaData("datetimeoffset", SqlDbType2.DateTimeOffset,
                   10, 0, 7, 0, SqlCompareOptions.None, false),                // SqlDbType2.DateTimeOffset
            };

        private string _name;
        private long _maxLength;
        private SqlDbType _sqlDbType;
        private byte _precision;
        private byte _scale;
        private long _locale;
        private SqlCompareOptions _compareOptions;
        private string _xmlSchemaCollectionDatabase;
        private string _xmlSchemaCollectionOwningSchema;
        private string _xmlSchemaCollectionName;
        private string _serverTypeName;
        private bool _partialLength;
        private Type _udtType;
        private bool _useServerDefault;
        private bool _isUniqueKey;
        private SortOrder _columnSortOrder;
        private int _sortOrdinal;


        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbType/*' />
        // scalar types constructor without tvp extended properties
        public SqlMetaData(string name, SqlDbType dbType)
        {
            Construct(name, dbType, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // scalar types constructor
        public SqlMetaData(
            string name, 
            SqlDbType dbType,
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLength/*' />
        // binary or string constructor with only max length
        // (for string types, locale and compare options will be picked up from the thread.
        public SqlMetaData(string name, SqlDbType dbType, long maxLength)
        {
            Construct(name, dbType, maxLength, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLengthUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // binary or string constructor with only max length and tvp extended properties
        // (for string types, locale and compare options will be picked up from the thread.
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            long maxLength, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, maxLength, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeUserDefinedType/*' />
        // udt ctor without tvp extended properties
        public SqlMetaData(string name, SqlDbType dbType,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType)
        {
            Construct(name, dbType, userDefinedType, null, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeUserDefinedTypeServerTypeName/*' />
        // udt ctor without tvp extended properties
        public SqlMetaData(string name, SqlDbType dbType,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType, string serverTypeName)
        {
            Construct(name, dbType, userDefinedType, serverTypeName, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeUserDefinedTypeServerTypeNameUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // udt ctor
        public SqlMetaData(
            string name, 
            SqlDbType dbType,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType, 
            string serverTypeName,
            bool useServerDefault, 
            bool isUniqueKey,
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, userDefinedType, serverTypeName, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypePrecisionScale/*' />
        // decimal ctor without tvp extended properties
        public SqlMetaData(string name, SqlDbType dbType, byte precision, byte scale)
        {
            Construct(name, dbType, precision, scale, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypePrecisionScaleUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // decimal ctor
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            byte precision, 
            byte scale, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, precision, scale, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLengthLocaleCompareOptions/*' />
        // string type constructor with locale and compare options, no tvp extended properties
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            long maxLength, 
            long locale,
            SqlCompareOptions compareOptions
        )
        {
            Construct(name, dbType, maxLength, locale, compareOptions, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLengthLocaleCompareOptionsUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // string type constructor with locale and compare options
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            long maxLength, 
            long locale,
            SqlCompareOptions compareOptions, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, maxLength, locale, compareOptions, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeDatabaseOwningSchemaObjectNameUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // typed xml ctor
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            string database, 
            string owningSchema,
            string objectName,
            bool useServerDefault, 
            bool isUniqueKey,
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            Construct(name, dbType, database, owningSchema, objectName, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLengthPrecisionScaleLocaleCompareOptionsUserDefinedType/*' />
        // everything except xml schema and tvp properties ctor
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            long maxLength, 
            byte precision,
            byte scale, 
            long locale, 
            SqlCompareOptions compareOptions,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType
        ) : this(
            name, 
            dbType, 
            maxLength, 
            precision, 
            scale, 
            locale, 
            compareOptions,
            userDefinedType, 
            DefaultUseServerDefault, 
            DefaultIsUniqueKey,
            DefaultColumnSortOrder, 
            DefaultSortOrdinal
        )
        {
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeMaxLengthPrecisionScaleLocaleCompareOptionsUserDefinedTypeUseServerDefaultIsUniqueKeyColumnSortOrderSortOrdinal/*' />
        // everything except xml schema ctor
        public SqlMetaData(
            string name, 
            SqlDbType dbType, 
            long maxLength, 
            byte precision,
            byte scale, 
            long localeId, 
            SqlCompareOptions compareOptions,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            switch (dbType)
            {
                case SqlDbType2.BigInt:
                case SqlDbType2.Image:
                case SqlDbType2.Timestamp:
                case SqlDbType2.Bit:
                case SqlDbType2.DateTime:
                case SqlDbType2.SmallDateTime:
                case SqlDbType2.Real:
                case SqlDbType2.Int:
                case SqlDbType2.Money:
                case SqlDbType2.SmallMoney:
                case SqlDbType2.Float:
                case SqlDbType2.UniqueIdentifier:
                case SqlDbType2.SmallInt:
                case SqlDbType2.TinyInt:
                case SqlDbType2.Xml:
                case SqlDbType2.Date:
                    Construct(name, dbType, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.Binary:
                case SqlDbType2.VarBinary:
                    Construct(name, dbType, maxLength, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.Char:
                case SqlDbType2.NChar:
                case SqlDbType2.NVarChar:
                case SqlDbType2.VarChar:
                    Construct(name, dbType, maxLength, localeId, compareOptions, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.NText:
                case SqlDbType2.Text:
                    // We should ignore user's max length and use Max instead to avoid exception
                    Construct(name, dbType, Max, localeId, compareOptions, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.Decimal:
                case SqlDbType2.Time:
                case SqlDbType2.DateTime2:
                case SqlDbType2.DateTimeOffset:
                    Construct(name, dbType, precision, scale, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.Variant:
                    Construct(name, dbType, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                case SqlDbType2.Udt:
                    Construct(name, dbType, userDefinedType, string.Empty, useServerDefault, isUniqueKey, columnSortOrder, sortOrdinal);
                    break;
                default:
                    throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/ctorNameDbTypeDatabaseOwningSchemaObjectName/*' />
        public SqlMetaData(string name, SqlDbType dbType, string database, string owningSchema, string objectName)
        {
            Construct(name, dbType, database, owningSchema, objectName, DefaultUseServerDefault, DefaultIsUniqueKey, DefaultColumnSortOrder, DefaultSortOrdinal);
        }

        // Most general constructor, should be able to initialize all SqlMetaData fields.(Used by SqlParameter)
        internal SqlMetaData(
            string name,
            SqlDbType sqlDBType,
            long maxLength,
            byte precision,
            byte scale,
            long localeId,
            SqlCompareOptions compareOptions,
            string xmlSchemaCollectionDatabase,
            string xmlSchemaCollectionOwningSchema,
            string xmlSchemaCollectionName,
            bool partialLength,
            Type udtType
        )
        {
            AssertNameIsValid(name);

            _name = name;
            _sqlDbType = sqlDBType;
            _maxLength = maxLength;
            _precision = precision;
            _scale = scale;
            _locale = localeId;
            _compareOptions = compareOptions;
            _xmlSchemaCollectionDatabase = xmlSchemaCollectionDatabase;
            _xmlSchemaCollectionOwningSchema = xmlSchemaCollectionOwningSchema;
            _xmlSchemaCollectionName = xmlSchemaCollectionName;
            _partialLength = partialLength;
            _udtType = udtType;
        }

        // Private constructor used to initialize default instance array elements.
        // DO NOT EXPOSE OUTSIDE THIS CLASS!  It performs no validation.
        private SqlMetaData(
            string name,
            SqlDbType sqlDbType,
            long maxLength,
            byte precision,
            byte scale,
            long localeId,
            SqlCompareOptions compareOptions,
            bool partialLength
        )
        {
            AssertNameIsValid(name);

            _name = name;
            _sqlDbType = sqlDbType;
            _maxLength = maxLength;
            _precision = precision;
            _scale = scale;
            _locale = localeId;
            _compareOptions = compareOptions;
            _partialLength = partialLength;
            _udtType = null;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/CompareOptions/*' />
        public SqlCompareOptions CompareOptions => _compareOptions;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/DbType/*' />
        public DbType DbType => s_sqlDbTypeToDbType[(int)_sqlDbType];

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/IsUniqueKey/*' />
        public bool IsUniqueKey => _isUniqueKey;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/LocaleId/*' />
        public long LocaleId => _locale;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/Max/*' />
        public static long Max => UnlimitedMaxLength;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/MaxLength/*' />
        public long MaxLength => _maxLength;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/Name/*' />
        public string Name => _name;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/Precision/*' />
        public byte Precision => _precision;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/Scale/*' />
        public byte Scale => _scale;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/SortOrder/*' />
        public SortOrder SortOrder => _columnSortOrder;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/SortOrdinal/*' />
        public int SortOrdinal => _sortOrdinal;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/SqlDbType/*' />
        public SqlDbType SqlDbType => _sqlDbType;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/Type/*' />
        public Type Type => _udtType;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/TypeName/*' />
        public string TypeName
        {
            get
            {
                if (_serverTypeName != null)
                {
                    return _serverTypeName;
                }
                else if (SqlDbType == SqlDbType2.Udt)
                {
                    return UdtTypeName;
                }
                else
                {
                    return s_defaults[(int)SqlDbType].Name;
                }
            }
        }

        internal string ServerTypeName => _serverTypeName;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/UseServerDefault/*' />
        public bool UseServerDefault => _useServerDefault;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/XmlSchemaCollectionDatabase/*' />
        public string XmlSchemaCollectionDatabase => _xmlSchemaCollectionDatabase;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/XmlSchemaCollectionName/*' />
        public string XmlSchemaCollectionName => _xmlSchemaCollectionName;

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/XmlSchemaCollectionOwningSchema/*' />
        public string XmlSchemaCollectionOwningSchema => _xmlSchemaCollectionOwningSchema;

        internal bool IsPartialLength => _partialLength;

        internal string UdtTypeName
        {
            get
            {
                if (SqlDbType != SqlDbType2.Udt)
                {
                    return null;
                }
                else if (_udtType == null)
                {
                    return null;
                }
                else
                {
                    return _udtType.FullName;
                }
            }
        }

        // Construction for all types that do not have variable attributes
        private void Construct(string name, SqlDbType dbType, bool useServerDefault, bool isUniqueKey, SortOrder columnSortOrder, int sortOrdinal)
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            // Check for absence of explicitly-allowed types to avoid unexpected additions when new types are added
            if (!(SqlDbType2.BigInt == dbType ||
                    SqlDbType2.Bit == dbType ||
                    SqlDbType2.DateTime == dbType ||
                    SqlDbType2.Date == dbType ||
                    SqlDbType2.DateTime2 == dbType ||
                    SqlDbType2.DateTimeOffset == dbType ||
                    SqlDbType2.Decimal == dbType ||
                    SqlDbType2.Float == dbType ||
                    SqlDbType2.Image == dbType ||
                    SqlDbType2.Int == dbType ||
                    SqlDbType2.Money == dbType ||
                    SqlDbType2.NText == dbType ||
                    SqlDbType2.Real == dbType ||
                    SqlDbType2.SmallDateTime == dbType ||
                    SqlDbType2.SmallInt == dbType ||
                    SqlDbType2.SmallMoney == dbType ||
                    SqlDbType2.Text == dbType ||
                    SqlDbType2.Time == dbType ||
                    SqlDbType2.Timestamp == dbType ||
                    SqlDbType2.TinyInt == dbType ||
                    SqlDbType2.UniqueIdentifier == dbType ||
                    SqlDbType2.Variant == dbType ||
                    SqlDbType2.Xml == dbType)
            )
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            SetDefaultsForType(dbType);

            if (SqlDbType2.NText == dbType || SqlDbType2.Text == dbType)
            {
                _locale = CultureInfo.CurrentCulture.LCID;
            }

            _name = name;
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        // Construction for all types that vary by user-specified length (not Udts)
        private void Construct(string name, SqlDbType dbType, long maxLength, bool useServerDefault, bool isUniqueKey, SortOrder columnSortOrder, int sortOrdinal)
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            long lLocale = 0;
            if (SqlDbType2.Char == dbType)
            {
                if (maxLength > MaxANSILength || maxLength < 0)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
                lLocale = CultureInfo.CurrentCulture.LCID;
            }
            else if (SqlDbType2.VarChar == dbType)
            {
                if ((maxLength > MaxANSILength || maxLength < 0) && maxLength != Max)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
                lLocale = CultureInfo.CurrentCulture.LCID;
            }
            else if (SqlDbType2.NChar == dbType)
            {
                if (maxLength > MaxUnicodeLength || maxLength < 0)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
                lLocale = CultureInfo.CurrentCulture.LCID;
            }
            else if (SqlDbType2.NVarChar == dbType)
            {
                if ((maxLength > MaxUnicodeLength || maxLength < 0) && maxLength != Max)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
                lLocale = CultureInfo.CurrentCulture.LCID;
            }
            else if (SqlDbType2.NText == dbType || SqlDbType2.Text == dbType)
            {
                // old-style lobs only allowed with Max length
                if (SqlMetaData.Max != maxLength)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
                lLocale = CultureInfo.CurrentCulture.LCID;
            }
            else if (SqlDbType2.Binary == dbType)
            {
                if (maxLength > MaxBinaryLength || maxLength < 0)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.VarBinary == dbType)
            {
                if ((maxLength > MaxBinaryLength || maxLength < 0) && maxLength != Max)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.Image == dbType)
            {
                // old-style lobs only allowed with Max length
                if (SqlMetaData.Max != maxLength)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            SetDefaultsForType(dbType);

            _name = name;
            _maxLength = maxLength;
            _locale = lLocale;
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        // Construction for string types with specified locale/compare options
        private void Construct(
            string name,
            SqlDbType dbType,
            long maxLength,
            long locale,
            SqlCompareOptions compareOptions,
            bool useServerDefault,
            bool isUniqueKey,
            SortOrder columnSortOrder,
            int sortOrdinal
        )
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            // Validate type and max length.
            if (SqlDbType2.Char == dbType)
            {
                if (maxLength > MaxANSILength || maxLength < 0)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.VarChar == dbType)
            {
                if ((maxLength > MaxANSILength || maxLength < 0) && maxLength != Max)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.NChar == dbType)
            {
                if (maxLength > MaxUnicodeLength || maxLength < 0)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.NVarChar == dbType)
            {
                if ((maxLength > MaxUnicodeLength || maxLength < 0) && maxLength != Max)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else if (SqlDbType2.NText == dbType || SqlDbType2.Text == dbType)
            {
                // old-style lobs only allowed with Max length
                if (SqlMetaData.Max != maxLength)
                {
                    throw ADP.Argument(StringsHelper.GetString(Strings.ADP_InvalidDataLength2, maxLength.ToString(CultureInfo.InvariantCulture)), nameof(maxLength));
                }
            }
            else
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            // Validate locale?

            // Validate compare options
            //    Binary sort must be by itself.
            //    Nothing else but the Ignore bits is allowed.
            if (SqlCompareOptions.BinarySort != compareOptions &&
                    0 != (~((int)SqlCompareOptions.IgnoreCase | (int)SqlCompareOptions.IgnoreNonSpace |
                            (int)SqlCompareOptions.IgnoreKanaType | (int)SqlCompareOptions.IgnoreWidth) &
                        (int)compareOptions)
            )
            {
                throw ADP.InvalidEnumerationValue(typeof(SqlCompareOptions), (int)compareOptions);
            }

            SetDefaultsForType(dbType);

            _name = name;
            _maxLength = maxLength;
            _locale = locale;
            _compareOptions = compareOptions;
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        // Construction for Decimal type and new 2008 Date/Time types
        private void Construct(
            string name, 
            SqlDbType dbType, 
            byte precision, 
            byte scale, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            if (SqlDbType2.Decimal == dbType)
            {
                if (precision > SqlDecimal.MaxPrecision || scale > precision)
                {
                    throw SQL.PrecisionValueOutOfRange(precision);
                }

                if (scale > SqlDecimal.MaxScale)
                {
                    throw SQL.ScaleValueOutOfRange(scale);
                }
            }
            else if (SqlDbType2.Time == dbType || SqlDbType2.DateTime2 == dbType || SqlDbType2.DateTimeOffset == dbType)
            {
                if (scale > MaxTimeScale)
                {
                    throw SQL.TimeScaleValueOutOfRange(scale);
                }
            }
            else
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            SetDefaultsForType(dbType);

            _name = name;
            _precision = precision;
            _scale = scale;
            if (SqlDbType2.Decimal == dbType)
            {
                _maxLength = s_maxLenFromPrecision[precision - 1];
            }
            else
            {
                _maxLength -= s_maxVarTimeLenOffsetFromScale[scale];
            }
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        // Construction for Udt type
        private void Construct(
            string name, 
            SqlDbType dbType,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType, 
            string serverTypeName, 
            bool useServerDefault,
            bool isUniqueKey, 
            SortOrder columnSortOrder, 
            int sortOrdinal
        )
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            if (SqlDbType2.Udt != dbType)
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            if (null == userDefinedType)
            {
                throw ADP.ArgumentNull(nameof(userDefinedType));
            }

            SetDefaultsForType(SqlDbType2.Udt);

            _name = name;
            _maxLength = SerializationHelperSql9.GetUdtMaxLength(userDefinedType);
            _udtType = userDefinedType;
            _serverTypeName = serverTypeName;
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        // Construction for Xml type
        private void Construct(
            string name, 
            SqlDbType dbType, 
            string database, 
            string owningSchema,
            string objectName, 
            bool useServerDefault, 
            bool isUniqueKey, 
            SortOrder columnSortOrder,
            int sortOrdinal
        )
        {
            AssertNameIsValid(name);

            ValidateSortOrder(columnSortOrder, sortOrdinal);

            if (SqlDbType2.Xml != dbType)
            {
                throw SQL.InvalidSqlDbTypeForConstructor(dbType);
            }

            if (null != database || null != owningSchema)
            {
                if (null == objectName)
                {
                    throw ADP.ArgumentNull(nameof(objectName));
                }
            }

            SetDefaultsForType(SqlDbType2.Xml);

            _name = name;
            _xmlSchemaCollectionDatabase = database;
            _xmlSchemaCollectionOwningSchema = owningSchema;
            _xmlSchemaCollectionName = objectName;
            _useServerDefault = useServerDefault;
            _isUniqueKey = isUniqueKey;
            _columnSortOrder = columnSortOrder;
            _sortOrdinal = sortOrdinal;
        }

        private void AssertNameIsValid(string name)
        {
            if (null == name)
            {
                throw ADP.ArgumentNull(nameof(name));
            }

            if (SmiMetaData.MaxNameLength < name.Length)
            {
                throw SQL.NameTooLong(nameof(name));
            }
        }

        private void ValidateSortOrder(SortOrder columnSortOrder, int sortOrdinal)
        {
            // Check that sort order is valid enum value.
            if (
                SortOrder.Unspecified != columnSortOrder &&
                SortOrder.Ascending != columnSortOrder &&
                SortOrder.Descending != columnSortOrder
            )
            {
                throw SQL.InvalidSortOrder(columnSortOrder);
            }

            // Must specify both sort order and ordinal, or neither
            if ((SortOrder.Unspecified == columnSortOrder) != (DefaultSortOrdinal == sortOrdinal))
            {
                throw SQL.MustSpecifyBothSortOrderAndOrdinal(columnSortOrder, sortOrdinal);
            }
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue27/*' />
        public short Adjust(short value)
        {
            if (SqlDbType2.SmallInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue28/*' />
        public int Adjust(int value)
        {
            if (SqlDbType2.Int != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue29/*' />
        public long Adjust(long value)
        {
            if (SqlDbType2.BigInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue31/*' />
        public float Adjust(float value)
        {
            if (SqlDbType2.Real != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue25/*' />
        public double Adjust(double value)
        {
            if (SqlDbType2.Float != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue32/*' />
        public string Adjust(string value)
        {
            if (SqlDbType2.Char == SqlDbType || SqlDbType2.NChar == SqlDbType)
            {
                // Don't pad null values
                if (null != value)
                {
                    // Pad if necessary
                    if (value.Length < MaxLength)
                    {
                        value = value.PadRight((int)MaxLength);
                    }
                }
            }
            else if (SqlDbType2.VarChar != SqlDbType &&
                     SqlDbType2.NVarChar != SqlDbType &&
                     SqlDbType2.Text != SqlDbType &&
                     SqlDbType2.NText != SqlDbType)
            {
                ThrowInvalidType();
            }

            // Handle null values after type check
            if (null == value)
            {
                return null;
            }

            if (value.Length > MaxLength && Max != MaxLength)
            {
                value = value.Remove((int)MaxLength, (int)(value.Length - MaxLength));
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue24/*' />
        public decimal Adjust(decimal value)
        {
            if (SqlDbType2.Decimal != SqlDbType &&
                SqlDbType2.Money != SqlDbType &&
                SqlDbType2.SmallMoney != SqlDbType)
            {
                ThrowInvalidType();
            }

            if (SqlDbType2.Decimal != SqlDbType)
            {
                VerifyMoneyRange(new SqlMoney(value));
                return value;
            }
            else
            {
                SqlDecimal sdValue = InternalAdjustSqlDecimal(new SqlDecimal(value));
                return sdValue.Value;
            }
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue22/*' />
        public DateTime Adjust(DateTime value)
        {
            if (SqlDbType2.DateTime == SqlDbType || SqlDbType2.SmallDateTime == SqlDbType)
            {
                VerifyDateTimeRange(value);
            }
            else if (SqlDbType2.DateTime2 == SqlDbType)
            {
                return new DateTime(InternalAdjustTimeTicks(value.Ticks));
            }
            else if (SqlDbType2.Date == SqlDbType)
            {
                return value.Date;
            }
            else
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue26/*' />
        public Guid Adjust(Guid value)
        {
            if (SqlDbType2.UniqueIdentifier != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue7/*' />
        public SqlBoolean Adjust(SqlBoolean value)
        {
            if (SqlDbType2.Bit != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue8/*' />
        public SqlByte Adjust(SqlByte value)
        {
            if (SqlDbType2.TinyInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue15/*' />
        public SqlInt16 Adjust(SqlInt16 value)
        {
            if (SqlDbType2.SmallInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue16/*' />
        public SqlInt32 Adjust(SqlInt32 value)
        {
            if (SqlDbType2.Int != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue17/*' />
        public SqlInt64 Adjust(SqlInt64 value)
        {
            if (SqlDbType2.BigInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue19/*' />
        public SqlSingle Adjust(SqlSingle value)
        {
            if (SqlDbType2.Real != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue13/*' />
        public SqlDouble Adjust(SqlDouble value)
        {
            if (SqlDbType2.Float != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue18/*' />
        public SqlMoney Adjust(SqlMoney value)
        {
            if (SqlDbType2.Money != SqlDbType && SqlDbType2.SmallMoney != SqlDbType)
            {
                ThrowInvalidType();
            }
            if (!value.IsNull)
            {
                VerifyMoneyRange(value);
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue11/*' />
        public SqlDateTime Adjust(SqlDateTime value)
        {
            if (SqlDbType2.DateTime != SqlDbType && SqlDbType2.SmallDateTime != SqlDbType)
            {
                ThrowInvalidType();
            }
            if (!value.IsNull)
            {
                VerifyDateTimeRange(value.Value);
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue12/*' />
        public SqlDecimal Adjust(SqlDecimal value)
        {
            if (SqlDbType2.Decimal != SqlDbType)
            {
                ThrowInvalidType();
            }
            return InternalAdjustSqlDecimal(value);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue20/*' />
        public SqlString Adjust(SqlString value)
        {
            if (SqlDbType2.Char == SqlDbType || SqlDbType2.NChar == SqlDbType)
            {
                //DBG.Assert(Max!=MaxLength, "SqlMetaData.Adjust(SqlString): Fixed-length type with Max length!");
                // Don't pad null values
                if (!value.IsNull)
                {
                    // Pad fixed-length types
                    if (value.Value.Length < MaxLength)
                    {
                        return new SqlString(value.Value.PadRight((int)MaxLength));
                    }
                }
            }
            else if (
                SqlDbType2.VarChar != SqlDbType &&
                SqlDbType2.NVarChar != SqlDbType &&
                SqlDbType2.Text != SqlDbType &&
                SqlDbType2.NText != SqlDbType
            )
            {
                ThrowInvalidType();
            }

            // Handle null values after type check
            if (value.IsNull)
            {
                return value;
            }

            // trim all types
            if (value.Value.Length > MaxLength && Max != MaxLength)
            {
                value = new SqlString(value.Value.Remove((int)MaxLength, (int)(value.Value.Length - MaxLength)));
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue6/*' />
        public SqlBinary Adjust(SqlBinary value)
        {
            if (SqlDbType2.Binary == SqlDbType || SqlDbType2.Timestamp == SqlDbType)
            {
                if (!value.IsNull)
                {
                    // Pad fixed-length types
                    if (value.Length < MaxLength)
                    {
                        byte[] rgbValue = value.Value;
                        byte[] rgbNewValue = new byte[MaxLength];
                        Buffer.BlockCopy(rgbValue, 0, rgbNewValue, 0, rgbValue.Length);
                        Array.Clear(rgbNewValue, rgbValue.Length, rgbNewValue.Length - rgbValue.Length);
                        return new SqlBinary(rgbNewValue);
                    }
                }
            }
            else if (SqlDbType2.VarBinary != SqlDbType && SqlDbType2.Image != SqlDbType)
            {
                ThrowInvalidType();
            }

            // Handle null values
            if (value.IsNull)
            {
                return value;
            }

            // trim all types
            if (value.Length > MaxLength && Max != MaxLength)
            {
                byte[] rgbValue = value.Value;
                byte[] rgbNewValue = new byte[MaxLength];
                Buffer.BlockCopy(rgbValue,0, rgbNewValue,0, (int)MaxLength);
                value = new SqlBinary(rgbNewValue);
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue14/*' />
        public SqlGuid Adjust(SqlGuid value)
        {
            if (SqlDbType2.UniqueIdentifier != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue10/*' />
        public SqlChars Adjust(SqlChars value)
        {
            if (SqlDbType2.Char == SqlDbType || SqlDbType2.NChar == SqlDbType)
            {
                //DBG.Assert(Max!=MaxLength, "SqlMetaData.Adjust(SqlChars): Fixed-length type with Max length!");
                // Don't pad null values
                if (null != value && !value.IsNull)
                {
                    // Pad fixed-length types
                    long oldLength = value.Length;
                    if (oldLength < MaxLength)
                    {
                        // Make sure buffer is long enough
                        if (value.MaxLength < MaxLength)
                        {
                            char[] rgchNew = new char[(int)MaxLength];
                            Array.Copy(value.Buffer, rgchNew, (int)oldLength);
                            value = new SqlChars(rgchNew);
                        }

                        // pad extra space
                        char[] rgchTemp = value.Buffer;
                        for (long i = oldLength; i < MaxLength; i++)
                        {
                            rgchTemp[i] = ' ';
                        }

                        value.SetLength(MaxLength);
                        return value;
                    }
                }
            }
            else if (
                SqlDbType2.VarChar != SqlDbType &&
                SqlDbType2.NVarChar != SqlDbType &&
                SqlDbType2.Text != SqlDbType &&
                SqlDbType2.NText != SqlDbType
            )
            {
                ThrowInvalidType();
            }

            // Handle null values after type check.
            if (null == value || value.IsNull)
            {
                return value;
            }

            // trim all types
            if (value.Length > MaxLength && Max != MaxLength)
            {
                value.SetLength(MaxLength);
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue9/*' />
        public SqlBytes Adjust(SqlBytes value)
        {
            if (SqlDbType2.Binary == SqlDbType || SqlDbType2.Timestamp == SqlDbType)
            {
                //DBG.Assert(Max!=MaxLength, "SqlMetaData.Adjust(SqlBytes): Fixed-length type with Max length!");
                // Don't pad null values
                if (null != value && !value.IsNull)
                {
                    // Pad fixed-length types
                    int oldLength = (int)value.Length;
                    if (oldLength < MaxLength)
                    {
                        // Make sure buffer is long enough
                        if (value.MaxLength < MaxLength)
                        {
                            byte[] rgbNew = new byte[MaxLength];
                            Buffer.BlockCopy(value.Buffer, 0,rgbNew,0, (int)oldLength);
                            value = new SqlBytes(rgbNew);
                        }

                        // pad extra space
                        byte[] rgbTemp = value.Buffer;
                        Array.Clear(rgbTemp, oldLength, rgbTemp.Length - oldLength);
                        value.SetLength(MaxLength);
                        return value;
                    }
                }
            }
            else if (SqlDbType2.VarBinary != SqlDbType && SqlDbType2.Image != SqlDbType)
            {
                ThrowInvalidType();
            }

            // Handle null values after type check.
            if (null == value || value.IsNull)
            {
                return value;
            }

            // trim all types
            if (value.Length > MaxLength && Max != MaxLength)
            {
                value.SetLength(MaxLength);
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue21/*' />
        public SqlXml Adjust(SqlXml value)
        {
            if (SqlDbType2.Xml != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue33/*' />
        public TimeSpan Adjust(TimeSpan value)
        {
            if (SqlDbType2.Time != SqlDbType)
            {
                ThrowInvalidType();
            }
            VerifyTimeRange(value);
            return new TimeSpan(InternalAdjustTimeTicks(value.Ticks));
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue23/*' />
        public DateTimeOffset Adjust(DateTimeOffset value)
        {
            if (SqlDbType2.DateTimeOffset != SqlDbType)
            {
                ThrowInvalidType();
            }
            return new DateTimeOffset(InternalAdjustTimeTicks(value.Ticks), value.Offset);
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue30/*' />
        public object Adjust(object value)
        {
            // Pass null references through
            if (null == value)
            {
                return null;
            }

            Type dataType = value.GetType();
            switch (Type.GetTypeCode(dataType))
            {
                case TypeCode.Boolean:
                    value = Adjust((bool)value);
                    break;
                case TypeCode.Byte:
                    value = Adjust((byte)value);
                    break;
                case TypeCode.Char:
                    value = Adjust((char)value);
                    break;
                case TypeCode.DateTime:
                    value = Adjust((DateTime)value);
                    break;
                case TypeCode.DBNull:    /* DBNull passes through as is for all types */
                    break;
                case TypeCode.Decimal:
                    value = Adjust((decimal)value);
                    break;
                case TypeCode.Double:
                    value = Adjust((double)value);
                    break;
                case TypeCode.Empty:
                    throw ADP.InvalidDataType(TypeCode.Empty);
                case TypeCode.Int16:
                    value = Adjust((short)value);
                    break;
                case TypeCode.Int32:
                    value = Adjust((int)value);
                    break;
                case TypeCode.Int64:
                    value = Adjust((long)value);
                    break;
                case TypeCode.SByte:
                    throw ADP.InvalidDataType(TypeCode.SByte);
                case TypeCode.Single:
                    value = Adjust((float)value);
                    break;
                case TypeCode.String:
                    value = Adjust((string)value);
                    break;
                case TypeCode.UInt16:
                    throw ADP.InvalidDataType(TypeCode.UInt16);
                case TypeCode.UInt32:
                    throw ADP.InvalidDataType(TypeCode.UInt32);
                case TypeCode.UInt64:
                    throw ADP.InvalidDataType(TypeCode.UInt64);
                case TypeCode.Object:
                    if (dataType == typeof(byte[]))
                    {
                        value = Adjust((byte[])value);
                    }
                    else if (dataType == typeof(char[]))
                    {
                        value = Adjust((char[])value);
                    }
                    else if (dataType == typeof(Guid))
                    {
                        value = Adjust((Guid)value);
                    }
                    else if (dataType == typeof(object))
                    {
                        throw ADP.InvalidDataType(TypeCode.UInt64);
                    }
                    else if (dataType == typeof(SqlBinary))
                    {
                        value = Adjust((SqlBinary)value);
                    }
                    else if (dataType == typeof(SqlBoolean))
                    {
                        value = Adjust((SqlBoolean)value);
                    }
                    else if (dataType == typeof(SqlByte))
                    {
                        value = Adjust((SqlByte)value);
                    }
                    else if (dataType == typeof(SqlDateTime))
                    {
                        value = Adjust((SqlDateTime)value);
                    }
                    else if (dataType == typeof(SqlDouble))
                    {
                        value = Adjust((SqlDouble)value);
                    }
                    else if (dataType == typeof(SqlGuid))
                    {
                        value = Adjust((SqlGuid)value);
                    }
                    else if (dataType == typeof(SqlInt16))
                    {
                        value = Adjust((SqlInt16)value);
                    }
                    else if (dataType == typeof(SqlInt32))
                    {
                        value = Adjust((SqlInt32)value);
                    }
                    else if (dataType == typeof(SqlInt64))
                    {
                        value = Adjust((SqlInt64)value);
                    }
                    else if (dataType == typeof(SqlMoney))
                    {
                        value = Adjust((SqlMoney)value);
                    }
                    else if (dataType == typeof(SqlDecimal))
                    {
                        value = Adjust((SqlDecimal)value);
                    }
                    else if (dataType == typeof(SqlSingle))
                    {
                        value = Adjust((SqlSingle)value);
                    }
                    else if (dataType == typeof(SqlString))
                    {
                        value = Adjust((SqlString)value);
                    }
                    else if (dataType == typeof(SqlChars))
                    {
                        value = Adjust((SqlChars)value);
                    }
                    else if (dataType == typeof(SqlBytes))
                    {
                        value = Adjust((SqlBytes)value);
                    }
                    else if (dataType == typeof(SqlXml))
                    {
                        value = Adjust((SqlXml)value);
                    }
                    else if (dataType == typeof(TimeSpan))
                    {
                        value = Adjust((TimeSpan)value);
                    }
                    else if (dataType == typeof(DateTimeOffset))
                    {
                        value = Adjust((DateTimeOffset)value);
                    }
                    else
                    {
                        // Handle UDTs?
                        throw ADP.UnknownDataType(dataType);
                    }
                    break;

                default:
                    throw ADP.UnknownDataTypeCode(dataType, Type.GetTypeCode(dataType));
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/InferFromValue/*' />
        public static SqlMetaData InferFromValue(object value, string name)
        {
            if (value == null)
            {
                throw ADP.ArgumentNull(nameof(value));
            }
            SqlMetaData smd;
            Type dataType = value.GetType();
            switch (Type.GetTypeCode(dataType))
            {
                case TypeCode.Boolean:
                    smd = new SqlMetaData(name, SqlDbType2.Bit);
                    break;
                case TypeCode.Byte:
                    smd = new SqlMetaData(name, SqlDbType2.TinyInt);
                    break;
                case TypeCode.Char:
                    smd = new SqlMetaData(name, SqlDbType2.NVarChar, 1);
                    break;
                case TypeCode.DateTime:
                    smd = new SqlMetaData(name, SqlDbType2.DateTime);
                    break;
                case TypeCode.DBNull:
                    throw ADP.InvalidDataType(TypeCode.DBNull);
                case TypeCode.Decimal:
                    {
                        SqlDecimal sd = new SqlDecimal((decimal)value);
                        smd = new SqlMetaData(name, SqlDbType2.Decimal, sd.Precision, sd.Scale);
                    }
                    break;
                case TypeCode.Double:
                    smd = new SqlMetaData(name, SqlDbType2.Float);
                    break;
                case TypeCode.Empty:
                    throw ADP.InvalidDataType(TypeCode.Empty);
                case TypeCode.Int16:
                    smd = new SqlMetaData(name, SqlDbType2.SmallInt);
                    break;
                case TypeCode.Int32:
                    smd = new SqlMetaData(name, SqlDbType2.Int);
                    break;
                case TypeCode.Int64:
                    smd = new SqlMetaData(name, SqlDbType2.BigInt);
                    break;
                case TypeCode.SByte:
                    throw ADP.InvalidDataType(TypeCode.SByte);
                case TypeCode.Single:
                    smd = new SqlMetaData(name, SqlDbType2.Real);
                    break;
                case TypeCode.String:
                    {
                        long maxLen = ((string)value).Length;
                        if (maxLen < 1)
                        {
                            maxLen = 1;
                        }
                        if (MaxUnicodeLength < maxLen)
                        {
                            maxLen = Max;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.NVarChar, maxLen);
                    }
                    break;
                case TypeCode.UInt16:
                    throw ADP.InvalidDataType(TypeCode.UInt16);
                case TypeCode.UInt32:
                    throw ADP.InvalidDataType(TypeCode.UInt32);
                case TypeCode.UInt64:
                    throw ADP.InvalidDataType(TypeCode.UInt64);
                case TypeCode.Object:
                    if (dataType == typeof(byte[]))
                    {
                        long maxLen = ((byte[])value).Length;
                        if (maxLen < 1)
                        {
                            maxLen = 1;
                        }
                        if (MaxBinaryLength < maxLen)
                        {
                            maxLen = Max;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.VarBinary, maxLen);
                    }
                    else if (dataType == typeof(char[]))
                    {
                        long maxLen = ((char[])value).Length;
                        if (maxLen < 1)
                        {
                            maxLen = 1;
                        }
                        if (MaxUnicodeLength < maxLen)
                        {
                            maxLen = Max;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.NVarChar, maxLen);
                    }
                    else if (dataType == typeof(Guid))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.UniqueIdentifier);
                    }
                    else if (dataType == typeof(object))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Variant);
                    }
                    else if (dataType == typeof(SqlBinary))
                    {
                        long maxLen;
                        SqlBinary sb = ((SqlBinary)value);
                        if (!sb.IsNull)
                        {
                            maxLen = sb.Length;
                            if (maxLen < 1)
                            {
                                maxLen = 1;
                            }
                            if (MaxBinaryLength < maxLen)
                            {
                                maxLen = Max;
                            }
                        }
                        else
                        {
                            maxLen = s_defaults[(int)SqlDbType2.VarBinary].MaxLength;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.VarBinary, maxLen);
                    }
                    else if (dataType == typeof(SqlBoolean))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Bit);
                    }
                    else if (dataType == typeof(SqlByte))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.TinyInt);
                    }
                    else if (dataType == typeof(SqlDateTime))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.DateTime);
                    }
                    else if (dataType == typeof(SqlDouble))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Float);
                    }
                    else if (dataType == typeof(SqlGuid))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.UniqueIdentifier);
                    }
                    else if (dataType == typeof(SqlInt16))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.SmallInt);
                    }
                    else if (dataType == typeof(SqlInt32))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Int);
                    }
                    else if (dataType == typeof(SqlInt64))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.BigInt);
                    }
                    else if (dataType == typeof(SqlMoney))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Money);
                    }
                    else if (dataType == typeof(SqlDecimal))
                    {
                        byte bPrec;
                        byte scale;
                        SqlDecimal sd = (SqlDecimal)value;
                        if (!sd.IsNull)
                        {
                            bPrec = sd.Precision;
                            scale = sd.Scale;
                        }
                        else
                        {
                            bPrec = s_defaults[(int)SqlDbType2.Decimal].Precision;
                            scale = s_defaults[(int)SqlDbType2.Decimal].Scale;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.Decimal, bPrec, scale);
                    }
                    else if (dataType == typeof(SqlSingle))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Real);
                    }
                    else if (dataType == typeof(SqlString))
                    {
                        SqlString ss = (SqlString)value;
                        if (!ss.IsNull)
                        {
                            long maxLen = ss.Value.Length;
                            if (maxLen < 1)
                            {
                                maxLen = 1;
                            }
                            if (maxLen > MaxUnicodeLength)
                            {
                                maxLen = Max;
                            }
                            smd = new SqlMetaData(name, SqlDbType2.NVarChar, maxLen, ss.LCID, ss.SqlCompareOptions);
                        }
                        else
                        {
                            smd = new SqlMetaData(name, SqlDbType2.NVarChar, s_defaults[(int)SqlDbType2.NVarChar].MaxLength);
                        }
                    }
                    else if (dataType == typeof(SqlChars))
                    {
                        long maxLen;
                        SqlChars sch = (SqlChars)value;
                        if (!sch.IsNull)
                        {
                            maxLen = sch.Length;
                            if (maxLen < 1)
                            {
                                maxLen = 1;
                            }
                            if (maxLen > MaxUnicodeLength)
                            {
                                maxLen = Max;
                            }
                        }
                        else
                        {
                            maxLen = s_defaults[(int)SqlDbType2.NVarChar].MaxLength;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.NVarChar, maxLen);
                    }
                    else if (dataType == typeof(SqlBytes))
                    {
                        long maxLen;
                        SqlBytes sb = (SqlBytes)value;
                        if (!sb.IsNull)
                        {
                            maxLen = sb.Length;
                            if (maxLen < 1)
                            {
                                maxLen = 1;
                            }
                            else if (MaxBinaryLength < maxLen)
                            {
                                maxLen = Max;
                            }
                        }
                        else
                        {
                            maxLen = s_defaults[(int)SqlDbType2.VarBinary].MaxLength;
                        }
                        smd = new SqlMetaData(name, SqlDbType2.VarBinary, maxLen);
                    }
                    else if (dataType == typeof(SqlXml))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Xml);
                    }
                    else if (dataType == typeof(TimeSpan))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.Time, 0, InferScaleFromTimeTicks(((TimeSpan)value).Ticks));
                    }
                    else if (dataType == typeof(DateTimeOffset))
                    {
                        smd = new SqlMetaData(name, SqlDbType2.DateTimeOffset, 0, InferScaleFromTimeTicks(((DateTimeOffset)value).Ticks));
                    }
                    else
                    {
                        throw ADP.UnknownDataType(dataType);
                    }
                    break;

                default:
                    throw ADP.UnknownDataTypeCode(dataType, Type.GetTypeCode(dataType));
            }

            return smd;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue1/*' />
        public bool Adjust(bool value)
        {
            if (SqlDbType2.Bit != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue2/*' />
        public byte Adjust(byte value)
        {
            if (SqlDbType2.TinyInt != SqlDbType)
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue3/*' />
        public byte[] Adjust(byte[] value)
        {
            if (SqlDbType2.Binary == SqlDbType || SqlDbType2.Timestamp == SqlDbType)
            {
                // Don't pad null values
                if (null != value)
                {
                    // Pad fixed-length types
                    if (value.Length < MaxLength)
                    {
                        byte[] rgbNewValue = new byte[MaxLength];
                        Buffer.BlockCopy(value, 0, rgbNewValue, 0, value.Length);
                        Array.Clear(rgbNewValue, value.Length, (int)rgbNewValue.Length - value.Length);
                        return rgbNewValue;
                    }
                }
            }
            else if (SqlDbType2.VarBinary != SqlDbType && SqlDbType2.Image != SqlDbType)
            {
                ThrowInvalidType();
            }

            // Handle null values after type check
            if (null == value)
            {
                return null;
            }

            // trim all types
            if (value.Length > MaxLength && Max != MaxLength)
            {
                byte[] rgbNewValue = new byte[MaxLength];
                Buffer.BlockCopy(value, 0, rgbNewValue, 0, (int)MaxLength);
                value = rgbNewValue;
            }

            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue4/*' />
        public char Adjust(char value)
        {
            if (SqlDbType2.Char == SqlDbType || SqlDbType2.NChar == SqlDbType)
            {
                if (1 != MaxLength)
                {
                    ThrowInvalidType();
                }
            }
            else if ((1 > MaxLength) ||  // char must have max length of at least 1
                    (SqlDbType2.VarChar != SqlDbType && SqlDbType2.NVarChar != SqlDbType &&
                    SqlDbType2.Text != SqlDbType && SqlDbType2.NText != SqlDbType)
                    )
            {
                ThrowInvalidType();
            }
            return value;
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient.Server/SqlMetaData.xml' path='docs/members[@name="SqlMetaData"]/AdjustValue5/*' />
        public char[] Adjust(char[] value)
        {
            if (SqlDbType2.Char == SqlDbType || SqlDbType2.NChar == SqlDbType)
            {
                // Don't pad null values
                if (null != value)
                {
                    // Pad fixed-length types
                    long oldLength = value.Length;
                    if (oldLength < MaxLength)
                    {
                        char[] rgchNew = new char[(int)MaxLength];
                        Array.Copy(value, rgchNew, (int)oldLength);

                        // pad extra space
                        for (long i = oldLength; i < rgchNew.Length; i++)
                        {
                            rgchNew[i] = ' ';
                        }
                        return rgchNew;
                    }
                }
            }
            else if (
                SqlDbType2.VarChar != SqlDbType && 
                SqlDbType2.NVarChar != SqlDbType &&
                SqlDbType2.Text != SqlDbType && 
                SqlDbType2.NText != SqlDbType
            )
            {
                ThrowInvalidType();
            }

            // Handle null values after type check
            if (null == value)
            {
                return null;
            }

            // trim all types
            if (value.Length > MaxLength && Max != MaxLength)
            {
                char[] rgchNewValue = new char[MaxLength];
                Array.Copy(value, rgchNewValue, (int)MaxLength);
                value = rgchNewValue;
            }

            return value;
        }


        internal static SqlMetaData GetPartialLengthMetaData(SqlMetaData md)
        {
            if (md.IsPartialLength == true)
            {
                return md;
            }
            if (md.SqlDbType == SqlDbType2.Xml)
            {
                ThrowInvalidType();     //Xml should always have IsPartialLength = true
            }
            if (
                md.SqlDbType == SqlDbType2.NVarChar ||
                md.SqlDbType == SqlDbType2.VarChar ||
                md.SqlDbType == SqlDbType2.VarBinary
            )
            {
                return new SqlMetaData(md.Name, md.SqlDbType, SqlMetaData.Max, 0, 0, md.LocaleId,
                    md.CompareOptions, null, null, null, true, md.Type);
            }
            else
            {
                return md;
            }
        }

        private static void ThrowInvalidType()
        {
            throw ADP.InvalidMetaDataValue();
        }

        private void VerifyDateTimeRange(DateTime value)
        {
            if (SqlDbType2.SmallDateTime == SqlDbType && (s_smallDateTimeMax < value || s_smallDateTimeMin > value))
            {
                ThrowInvalidType();
            }
        }

        private void VerifyMoneyRange(SqlMoney value)
        {
            if (SqlDbType2.SmallMoney == SqlDbType && ((s_smallMoneyMax < value).Value || (s_smallMoneyMin > value).Value))
            {
                ThrowInvalidType();
            }
        }

        private SqlDecimal InternalAdjustSqlDecimal(SqlDecimal value)
        {
            if (!value.IsNull && (value.Precision != Precision || value.Scale != Scale))
            {
                // Force truncation if target scale is smaller than actual scale.
                if (value.Scale != Scale)
                {
                    value = SqlDecimal.AdjustScale(value, Scale - value.Scale, false /* Don't round, truncate. */);
                }
                return SqlDecimal.ConvertToPrecScale(value, Precision, Scale);
            }

            return value;
        }

        private void VerifyTimeRange(TimeSpan value)
        {
            if (SqlDbType2.Time == SqlDbType && (s_timeMin > value || value > s_timeMax))
            {
                ThrowInvalidType();
            }
        }

        private long InternalAdjustTimeTicks(long ticks)
        {
            return (ticks / s_unitTicksFromScale[Scale] * s_unitTicksFromScale[Scale]);
        }

        private static byte InferScaleFromTimeTicks(long ticks)
        {
            for (byte scale = 0; scale < MaxTimeScale; ++scale)
            {
                if ((ticks / s_unitTicksFromScale[scale] * s_unitTicksFromScale[scale]) == ticks)
                {
                    return scale;
                }
            }
            return MaxTimeScale;
        }

        private void SetDefaultsForType(SqlDbType dbType)
        {
            if (SqlDbType2.BigInt <= dbType && SqlDbType2.DateTimeOffset >= dbType)
            {
                SqlMetaData smdDflt = s_defaults[(int)dbType];
                _sqlDbType = dbType;
                _maxLength = smdDflt.MaxLength;
                _precision = smdDflt.Precision;
                _scale = smdDflt.Scale;
                _locale = smdDflt.LocaleId;
                _compareOptions = smdDflt.CompareOptions;
            }
        }
    }
}
