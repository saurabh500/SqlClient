// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Microsoft.Data.SqlClient.Server
{
    // DESIGN NOTES
    //
    //  The following classes are a tight inheritance hierarchy, and are not designed for
    //  being inherited outside of this file.  Instances are guaranteed to be immutable, and
    //  outside classes rely on this fact.
    //
    //  The various levels may not all be used outside of this file, but for clarity of purpose
    //  they are all useful distinctions to make.
    //
    //  In general, moving lower in the type hierarchy exposes less portable values.  Thus,
    //  the root metadata can be readily shared across different (MSSQL) servers and clients,
    //  while QueryMetaData has attributes tied to a specific query, running against specific
    //  data storage on a specific server.
    //
    //  The SmiMetaData hierarchy does not do data validation on retail builds!  It will assert
    //  that the values passed to it have been validated externally, however.
    //


    // SmiMetaData
    //
    //  Root of the hierarchy.
    //  Represents the minimal amount of metadata required to represent any Sql Server datum
    //  without any references to any particular server or schema (thus, no server-specific multi-part names).
    //  It could be used to communicate solely between two disconnected clients, for instance.
    //
    //  NOTE: It currently does not contain sufficient information to describe typed XML, since we
    //      don't have a good server-independent mechanism for such.
    //
    //  This class is also used as implementation for the public SqlMetaData class.
    internal class SmiMetaData
    {
        private SqlDbType _databaseType;          // Main enum that determines what is valid for other attributes.
        private long _maxLength;             // Varies for variable-length types, others are fixed value per type
        private byte _precision;             // Varies for SqlDbType2.Decimal, others are fixed value per type
        private byte _scale;                 // Varies for SqlDbType2.Decimal, others are fixed value per type
        private long _localeId;              // Valid only for character types, others are 0
        private SqlCompareOptions _compareOptions;        // Valid only for character types, others are SqlCompareOptions.Default
        private Type _clrType;               // Varies for SqlDbType2.Udt, others are fixed value per type.
        private string _udtAssemblyQualifiedName;           // Valid only for UDT types when _clrType is not available
        private bool _isMultiValued;         // Multiple instances per value? (I.e. tables, arrays)
        private IList<SmiExtendedMetaData> _fieldMetaData;         // Metadata of fields for structured types
        private SmiMetaDataPropertyCollection _extendedProperties;  // Extended properties, Key columns, sort order, etc.

        // Limits for attributes (SmiMetaData will assert that these limits as applicable in constructor)
        internal const long UnlimitedMaxLengthIndicator = -1;  // unlimited (except by implementation) max-length.
        internal const long MaxUnicodeCharacters = 4000;        // Maximum for limited type
        internal const long MaxANSICharacters = 8000;           // Maximum for limited type
        internal const long MaxBinaryLength = 8000;             // Maximum for limited type
        internal const int MinPrecision = 1;       // SqlDecimal defines max precision
        internal const int MinScale = 0;            // SqlDecimal defines max scale
        internal const int MaxTimeScale = 7;        // Max scale for time, datetime2, and datetimeoffset
        internal static readonly DateTime MaxSmallDateTime = new DateTime(2079, 06, 06, 23, 59, 29, 998);
        internal static readonly DateTime MinSmallDateTime = new DateTime(1899, 12, 31, 23, 59, 29, 999);
        internal static readonly SqlMoney MaxSmallMoney = new SqlMoney(((decimal)int.MaxValue) / 10000);
        internal static readonly SqlMoney MinSmallMoney = new SqlMoney(((decimal)int.MinValue) / 10000);
        internal const SqlCompareOptions DefaultStringCompareOptions = SqlCompareOptions.IgnoreCase
                                        | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth;

        internal const long MaxNameLength = 128;        // maximum length in the server is 128.
        private static readonly IList<SmiExtendedMetaData> s_emptyFieldList = new List<SmiExtendedMetaData>().AsReadOnly();

        // Precision to max length lookup table
        private static readonly byte[] s_maxLenFromPrecision = new byte[] {5,5,5,5,5,5,5,5,5,9,9,9,9,9,
            9,9,9,9,9,13,13,13,13,13,13,13,13,13,17,17,17,17,17,17,17,17,17,17};

        // Scale offset to max length lookup table
        private static readonly byte[] s_maxVarTimeLenOffsetFromScale = new byte[] { 2, 2, 2, 1, 1, 0, 0, 0 };

        // Defaults
        //    SmiMetaData(SqlDbType,                  MaxLen,                     Prec, Scale,  CompareOptions)
        internal static readonly SmiMetaData DefaultBigInt = new SmiMetaData(SqlDbType2.BigInt, 8, 19, 0, SqlCompareOptions.None);     // SqlDbType2.BigInt
        internal static readonly SmiMetaData DefaultBinary = new SmiMetaData(SqlDbType2.Binary, 1, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Binary
        internal static readonly SmiMetaData DefaultBit = new SmiMetaData(SqlDbType2.Bit, 1, 1, 0, SqlCompareOptions.None);     // SqlDbType2.Bit
        internal static readonly SmiMetaData DefaultChar_NoCollation = new SmiMetaData(SqlDbType2.Char, 1, 0, 0, DefaultStringCompareOptions);// SqlDbType2.Char
        internal static readonly SmiMetaData DefaultDateTime = new SmiMetaData(SqlDbType2.DateTime, 8, 23, 3, SqlCompareOptions.None);     // SqlDbType2.DateTime
        internal static readonly SmiMetaData DefaultDecimal = new SmiMetaData(SqlDbType2.Decimal, 9, 18, 0, SqlCompareOptions.None);     // SqlDbType2.Decimal
        internal static readonly SmiMetaData DefaultFloat = new SmiMetaData(SqlDbType2.Float, 8, 53, 0, SqlCompareOptions.None);     // SqlDbType2.Float
        internal static readonly SmiMetaData DefaultImage = new SmiMetaData(SqlDbType2.Image, UnlimitedMaxLengthIndicator, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Image
        internal static readonly SmiMetaData DefaultInt = new SmiMetaData(SqlDbType2.Int, 4, 10, 0, SqlCompareOptions.None);     // SqlDbType2.Int
        internal static readonly SmiMetaData DefaultMoney = new SmiMetaData(SqlDbType2.Money, 8, 19, 4, SqlCompareOptions.None);     // SqlDbType2.Money
        internal static readonly SmiMetaData DefaultNChar_NoCollation = new SmiMetaData(SqlDbType2.NChar, 1, 0, 0, DefaultStringCompareOptions);// SqlDbType2.NChar
        internal static readonly SmiMetaData DefaultNText_NoCollation = new SmiMetaData(SqlDbType2.NText, UnlimitedMaxLengthIndicator, 0, 0, DefaultStringCompareOptions);// SqlDbType2.NText
        internal static readonly SmiMetaData DefaultNVarChar_NoCollation = new SmiMetaData(SqlDbType2.NVarChar, MaxUnicodeCharacters, 0, 0, DefaultStringCompareOptions);// SqlDbType2.NVarChar
        internal static readonly SmiMetaData DefaultReal = new SmiMetaData(SqlDbType2.Real, 4, 24, 0, SqlCompareOptions.None);     // SqlDbType2.Real
        internal static readonly SmiMetaData DefaultUniqueIdentifier = new SmiMetaData(SqlDbType2.UniqueIdentifier, 16, 0, 0, SqlCompareOptions.None);     // SqlDbType2.UniqueIdentifier
        internal static readonly SmiMetaData DefaultSmallDateTime = new SmiMetaData(SqlDbType2.SmallDateTime, 4, 16, 0, SqlCompareOptions.None);     // SqlDbType2.SmallDateTime
        internal static readonly SmiMetaData DefaultSmallInt = new SmiMetaData(SqlDbType2.SmallInt, 2, 5, 0, SqlCompareOptions.None);     // SqlDbType2.SmallInt
        internal static readonly SmiMetaData DefaultSmallMoney = new SmiMetaData(SqlDbType2.SmallMoney, 4, 10, 4, SqlCompareOptions.None);     // SqlDbType2.SmallMoney
        internal static readonly SmiMetaData DefaultText_NoCollation = new SmiMetaData(SqlDbType2.Text, UnlimitedMaxLengthIndicator, 0, 0, DefaultStringCompareOptions);// SqlDbType2.Text
        internal static readonly SmiMetaData DefaultTimestamp = new SmiMetaData(SqlDbType2.Timestamp, 8, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Timestamp
        internal static readonly SmiMetaData DefaultTinyInt = new SmiMetaData(SqlDbType2.TinyInt, 1, 3, 0, SqlCompareOptions.None);     // SqlDbType2.TinyInt
        internal static readonly SmiMetaData DefaultVarBinary = new SmiMetaData(SqlDbType2.VarBinary, MaxBinaryLength, 0, 0, SqlCompareOptions.None);     // SqlDbType2.VarBinary
        internal static readonly SmiMetaData DefaultVarChar_NoCollation = new SmiMetaData(SqlDbType2.VarChar, MaxANSICharacters, 0, 0, DefaultStringCompareOptions);// SqlDbType2.VarChar
        internal static readonly SmiMetaData DefaultVariant = new SmiMetaData(SqlDbType2.Variant, 8016, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Variant
        internal static readonly SmiMetaData DefaultXml = new SmiMetaData(SqlDbType2.Xml, UnlimitedMaxLengthIndicator, 0, 0, DefaultStringCompareOptions);// SqlDbType2.Xml
        internal static readonly SmiMetaData DefaultUdt_NoType = new SmiMetaData(SqlDbType2.Udt, 0, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Udt
        internal static readonly SmiMetaData DefaultStructured = new SmiMetaData(SqlDbType2.Structured, 0, 0, 0, SqlCompareOptions.None);     // SqlDbType2.Structured
        internal static readonly SmiMetaData DefaultDate = new SmiMetaData(SqlDbType2.Date, 3, 10, 0, SqlCompareOptions.None);     // SqlDbType2.Date
        internal static readonly SmiMetaData DefaultTime = new SmiMetaData(SqlDbType2.Time, 5, 0, 7, SqlCompareOptions.None);     // SqlDbType2.Time
        internal static readonly SmiMetaData DefaultDateTime2 = new SmiMetaData(SqlDbType2.DateTime2, 8, 0, 7, SqlCompareOptions.None);     // SqlDbType2.DateTime2
        internal static readonly SmiMetaData DefaultDateTimeOffset = new SmiMetaData(SqlDbType2.DateTimeOffset, 10, 0, 7, SqlCompareOptions.None);     // SqlDbType2.DateTimeOffset
        // No default for generic UDT

        // character defaults hook thread-local culture to get collation
        internal static SmiMetaData DefaultChar =>
            new SmiMetaData(
                DefaultChar_NoCollation.SqlDbType,
                DefaultChar_NoCollation.MaxLength,
                DefaultChar_NoCollation.Precision,
                DefaultChar_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );


        internal static SmiMetaData DefaultNChar =>
            new SmiMetaData(
                DefaultNChar_NoCollation.SqlDbType,
                DefaultNChar_NoCollation.MaxLength,
                DefaultNChar_NoCollation.Precision,
                DefaultNChar_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );

        internal static SmiMetaData DefaultNText => 
            new SmiMetaData(
                DefaultNText_NoCollation.SqlDbType,
                DefaultNText_NoCollation.MaxLength,
                DefaultNText_NoCollation.Precision,
                DefaultNText_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );

        internal static SmiMetaData DefaultNVarChar => 
            new SmiMetaData(
                DefaultNVarChar_NoCollation.SqlDbType,
                DefaultNVarChar_NoCollation.MaxLength,
                DefaultNVarChar_NoCollation.Precision,
                DefaultNVarChar_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );

        internal static SmiMetaData DefaultText =>
            new SmiMetaData(
                DefaultText_NoCollation.SqlDbType,
                DefaultText_NoCollation.MaxLength,
                DefaultText_NoCollation.Precision,
                DefaultText_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );

        internal static SmiMetaData DefaultVarChar =>
            new SmiMetaData(
                DefaultVarChar_NoCollation.SqlDbType,
                DefaultVarChar_NoCollation.MaxLength,
                DefaultVarChar_NoCollation.Precision,
                DefaultVarChar_NoCollation.Scale,
                CultureInfo.CurrentCulture.LCID,
                SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth,
                null
            );

        // The one and only constructor for use by outside code.
        //
        //  Parameters that matter for given values of dbType (other parameters are ignored in favor of internal defaults).
        //  Thus, if dbType parameter value is SqlDbType2.Decimal, the values of precision and scale passed in are used, but
        //  maxLength, localeId, compareOptions, etc are set to defaults for the Decimal type:
        //      SqlDbType2.BigInt:               dbType
        //      SqlDbType2.Binary:               dbType, maxLength
        //      SqlDbType2.Bit:                  dbType
        //      SqlDbType2.Char:                 dbType, maxLength, localeId, compareOptions
        //      SqlDbType2.DateTime:             dbType
        //      SqlDbType2.Decimal:              dbType, precision, scale
        //      SqlDbType2.Float:                dbType
        //      SqlDbType2.Image:                dbType
        //      SqlDbType2.Int:                  dbType
        //      SqlDbType2.Money:                dbType
        //      SqlDbType2.NChar:                dbType, maxLength, localeId, compareOptions
        //      SqlDbType2.NText:                dbType, localeId, compareOptions
        //      SqlDbType2.NVarChar:             dbType, maxLength, localeId, compareOptions
        //      SqlDbType2.Real:                 dbType
        //      SqlDbType2.UniqueIdentifier:     dbType
        //      SqlDbType2.SmallDateTime:        dbType
        //      SqlDbType2.SmallInt:             dbType
        //      SqlDbType2.SmallMoney:           dbType
        //      SqlDbType2.Text:                 dbType, localeId, compareOptions
        //      SqlDbType2.Timestamp:            dbType
        //      SqlDbType2.TinyInt:              dbType
        //      SqlDbType2.VarBinary:            dbType, maxLength
        //      SqlDbType2.VarChar:              dbType, maxLength, localeId, compareOptions
        //      SqlDbType2.Variant:              dbType
        //      PlaceHolder for value 24
        //      SqlDbType2.Xml:                  dbType
        //      Placeholder for value 26
        //      Placeholder for value 27
        //      Placeholder for value 28
        //      SqlDbType2.Udt:                  dbType, userDefinedType
        //

        // SMI V100 (aka V3) constructor.  Superceded in V200.
        internal SmiMetaData(
            SqlDbType dbType,
            long maxLength,
            byte precision,
            byte scale,
            long localeId,
            SqlCompareOptions compareOptions,
#if NET6_0_OR_GREATER
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
#endif
            Type userDefinedType
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                false,
                null,
                null
            )
        {
        }

        // SMI V200 ctor.
        internal SmiMetaData(
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
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldTypes,
            SmiMetaDataPropertyCollection extendedProperties
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                null,
                isMultiValued,
                fieldTypes,
                extendedProperties
            )
        {
        }

        // SMI V220 ctor.
        internal SmiMetaData(
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
            string udtAssemblyQualifiedName,
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldTypes,
            SmiMetaDataPropertyCollection extendedProperties
        )
        {
            Debug.Assert(IsSupportedDbType(dbType), "Invalid SqlDbType: " + dbType);

            SetDefaultsForType(dbType);

            switch (dbType)
            {
                case SqlDbType2.BigInt:
                case SqlDbType2.Bit:
                case SqlDbType2.DateTime:
                case SqlDbType2.Float:
                case SqlDbType2.Image:
                case SqlDbType2.Int:
                case SqlDbType2.Money:
                case SqlDbType2.Real:
                case SqlDbType2.SmallDateTime:
                case SqlDbType2.SmallInt:
                case SqlDbType2.SmallMoney:
                case SqlDbType2.Timestamp:
                case SqlDbType2.TinyInt:
                case SqlDbType2.UniqueIdentifier:
                case SqlDbType2.Variant:
                case SqlDbType2.Xml:
                case SqlDbType2.Date:
                    break;
                case SqlDbType2.Binary:
                case SqlDbType2.VarBinary:
                    _maxLength = maxLength;
                    break;
                case SqlDbType2.Char:
                case SqlDbType2.NChar:
                case SqlDbType2.NVarChar:
                case SqlDbType2.VarChar:
                    // locale and compare options are not validated until they get to the server
                    _maxLength = maxLength;
                    _localeId = localeId;
                    _compareOptions = compareOptions;
                    break;
                case SqlDbType2.NText:
                case SqlDbType2.Text:
                    _localeId = localeId;
                    _compareOptions = compareOptions;
                    break;
                case SqlDbType2.Decimal:
                    Debug.Assert(MinPrecision <= precision && SqlDecimal.MaxPrecision >= precision, "Invalid precision: " + precision);
                    Debug.Assert(MinScale <= scale && SqlDecimal.MaxScale >= scale, "Invalid scale: " + scale);
                    Debug.Assert(scale <= precision, "Precision: " + precision + " greater than scale: " + scale);
                    _precision = precision;
                    _scale = scale;
                    _maxLength = s_maxLenFromPrecision[precision - 1];
                    break;
                case SqlDbType2.Udt:
                    // For SqlParameter, both userDefinedType and udtAssemblyQualifiedName can be NULL,
                    // so we are checking only maxLength if it will be used (i.e. userDefinedType is NULL)
                    Debug.Assert((userDefinedType != null) || (0 <= maxLength || UnlimitedMaxLengthIndicator == maxLength),
                             $"SmiMetaData.ctor: Udt name={udtAssemblyQualifiedName}, maxLength={maxLength}");
                    // Type not validated until matched to a server.  Could be null if extended metadata supplies three-part name!
                    _clrType = userDefinedType;
                    if (userDefinedType != null)
                    {
                        _maxLength = SerializationHelperSql9.GetUdtMaxLength(userDefinedType);
                    }
                    else
                    {
                        _maxLength = maxLength;
                    }
                    _udtAssemblyQualifiedName = udtAssemblyQualifiedName;
                    break;
                case SqlDbType2.Structured:
                    if (fieldTypes != null)
                    {
                        _fieldMetaData = (new List<SmiExtendedMetaData>(fieldTypes)).AsReadOnly();
                    }
                    _isMultiValued = isMultiValued;
                    _maxLength = _fieldMetaData.Count;
                    break;
                case SqlDbType2.Time:
                    Debug.Assert(MinScale <= scale && scale <= MaxTimeScale, "Invalid time scale: " + scale);
                    _scale = scale;
                    _maxLength = 5 - s_maxVarTimeLenOffsetFromScale[scale];
                    break;
                case SqlDbType2.DateTime2:
                    Debug.Assert(MinScale <= scale && scale <= MaxTimeScale, "Invalid time scale: " + scale);
                    _scale = scale;
                    _maxLength = 8 - s_maxVarTimeLenOffsetFromScale[scale];
                    break;
                case SqlDbType2.DateTimeOffset:
                    Debug.Assert(MinScale <= scale && scale <= MaxTimeScale, "Invalid time scale: " + scale);
                    _scale = scale;
                    _maxLength = 10 - s_maxVarTimeLenOffsetFromScale[scale];
                    break;
                default:
                    Debug.Fail("How in the world did we get here? :" + dbType);
                    break;
            }

            if (extendedProperties != null)
            {
                extendedProperties.SetReadOnly();
                _extendedProperties = extendedProperties;
            }

            // properties and fields must meet the following conditions at this point:
            //  1) not null
            //  2) read only
            //  3) same number of columns in each list (0 count acceptable for properties that are "unused")
            Debug.Assert(_extendedProperties != null && _extendedProperties.IsReadOnly, "SmiMetaData.ctor: _extendedProperties is " + (_extendedProperties !=  null? "writable" : "null"));
            Debug.Assert(_fieldMetaData != null && _fieldMetaData.IsReadOnly, "SmiMetaData.ctor: _fieldMetaData is " + (_fieldMetaData != null ? "writable" : "null"));
#if DEBUG
            ((SmiDefaultFieldsProperty)_extendedProperties[SmiPropertySelector.DefaultFields]).CheckCount(_fieldMetaData.Count);
            ((SmiOrderProperty)_extendedProperties[SmiPropertySelector.SortOrder]).CheckCount(_fieldMetaData.Count);
            ((SmiUniqueKeyProperty)_extendedProperties[SmiPropertySelector.UniqueKey]).CheckCount(_fieldMetaData.Count);
#endif
        }

        internal bool IsValidMaxLengthForCtorGivenType(SqlDbType dbType, long maxLength)
        {
            bool result = true;
            switch (dbType)
            {
                case SqlDbType2.BigInt:
                case SqlDbType2.Bit:
                case SqlDbType2.DateTime:
                case SqlDbType2.Float:
                case SqlDbType2.Image:
                case SqlDbType2.Int:
                case SqlDbType2.Money:
                case SqlDbType2.Real:
                case SqlDbType2.SmallDateTime:
                case SqlDbType2.SmallInt:
                case SqlDbType2.SmallMoney:
                case SqlDbType2.Timestamp:
                case SqlDbType2.TinyInt:
                case SqlDbType2.UniqueIdentifier:
                case SqlDbType2.Variant:
                case SqlDbType2.Xml:
                case SqlDbType2.NText:
                case SqlDbType2.Text:
                case SqlDbType2.Decimal:
                case SqlDbType2.Udt:
                case SqlDbType2.Structured:
                case SqlDbType2.Date:
                case SqlDbType2.Time:
                case SqlDbType2.DateTime2:
                case SqlDbType2.DateTimeOffset:
                    break;
                case SqlDbType2.Binary:
                    result = 0 < maxLength && MaxBinaryLength >= maxLength;
                    break;
                case SqlDbType2.VarBinary:
                    result = UnlimitedMaxLengthIndicator == maxLength || (0 < maxLength && MaxBinaryLength >= maxLength);
                    break;
                case SqlDbType2.Char:
                    result = 0 < maxLength && MaxANSICharacters >= maxLength;
                    break;
                case SqlDbType2.NChar:
                    result = 0 < maxLength && MaxUnicodeCharacters >= maxLength;
                    break;
                case SqlDbType2.NVarChar:
                    result = UnlimitedMaxLengthIndicator == maxLength || (0 < maxLength && MaxUnicodeCharacters >= maxLength);
                    break;
                case SqlDbType2.VarChar:
                    result = UnlimitedMaxLengthIndicator == maxLength || (0 < maxLength && MaxANSICharacters >= maxLength);
                    break;
                default:
                    Debug.Fail("How in the world did we get here? :" + dbType);
                    break;
            }

            return result;
        }

        // Sql-style compare options for character types.
        internal SqlCompareOptions CompareOptions => _compareOptions;

        // LCID for type.  0 for non-character types.
        internal long LocaleId => _localeId;

        // Units of length depend on type.
        //  NVarChar, NChar, NText: # of Unicode characters
        //  Everything else: # of bytes
        internal long MaxLength => _maxLength;

        internal byte Precision => _precision;

        internal byte Scale => _scale;

        internal SqlDbType SqlDbType => _databaseType;

        // Clr Type instance for user-defined types
        internal Type Type
        {
            get
            {
                // Fault-in UDT clr types on access if have assembly-qualified name
                if (null == _clrType && SqlDbType2.Udt == _databaseType && _udtAssemblyQualifiedName != null)
                {
                    _clrType = Type.GetType(_udtAssemblyQualifiedName, true);
                }
                return _clrType;
            }
        }

        // Clr Type instance for user-defined types in cases where we don't want to throw if the assembly isn't available
        internal Type TypeWithoutThrowing
        {
            get
            {
                // Fault-in UDT clr types on access if have assembly-qualified name
                if (null == _clrType && SqlDbType2.Udt == _databaseType && _udtAssemblyQualifiedName != null)
                {
                    _clrType = Type.GetType(_udtAssemblyQualifiedName, false);
                }
                return _clrType;
            }
        }

        internal string TypeName
        {
            get
            {
                string result;
                if (SqlDbType2.Udt == _databaseType)
                {
                    Debug.Assert(string.Empty == s_typeNameByDatabaseType[(int)_databaseType], "unexpected udt?");
                    result = Type.FullName;
                }
                else
                {
                    result = s_typeNameByDatabaseType[(int)_databaseType];
                    Debug.Assert(result != null, "unknown type name?");
                }
                return result;
            }
        }

        internal string AssemblyQualifiedName
        {
            get
            {
                string result = null;
                if (SqlDbType2.Udt == _databaseType)
                {
                    // Fault-in assembly-qualified name if type is available
                    if (_udtAssemblyQualifiedName == null && _clrType != null)
                    {
                        _udtAssemblyQualifiedName = _clrType.AssemblyQualifiedName;
                    }
                    result = _udtAssemblyQualifiedName;
                }
                return result;
            }
        }

        internal bool IsMultiValued => _isMultiValued;

        // Returns read-only list of field metadata
        internal IList<SmiExtendedMetaData> FieldMetaData => _fieldMetaData;

        // Returns read-only list of extended properties
        internal SmiMetaDataPropertyCollection ExtendedProperties => _extendedProperties;

        internal static bool IsSupportedDbType(SqlDbType dbType)
        {
            // Hole in SqlDbTypes between Xml and Udt for non-WinFS scenarios.
            return (SqlDbType2.BigInt <= dbType && SqlDbType2.Xml >= dbType) ||
                    (SqlDbType2.Udt <= dbType && SqlDbType2.DateTimeOffset >= dbType);
        }

        // Only correct access point for defaults per SqlDbType2.
        internal static SmiMetaData GetDefaultForType(SqlDbType dbType)
        {
            Debug.Assert(IsSupportedDbType(dbType), "Unsupported SqlDbtype: " + dbType);

            return s_defaultValues[(int)dbType];
        }

        // Private constructor used only to initialize default instance array elements.
        // DO NOT EXPOSE OUTSIDE THIS CLASS!
        private SmiMetaData(
            SqlDbType sqlDbType,
            long maxLength,
            byte precision,
            byte scale,
            SqlCompareOptions compareOptions
        )
        {
            _databaseType = sqlDbType;
            _maxLength = maxLength;
            _precision = precision;
            _scale = scale;
            _compareOptions = compareOptions;

            // defaults are the same for all types for the following attributes.
            _localeId = 0;
            _clrType = null;
            _isMultiValued = false;
            _fieldMetaData = s_emptyFieldList;
            _extendedProperties = SmiMetaDataPropertyCollection.s_emptyInstance;
        }

        // static array of default-valued metadata ordered by corresponding SqlDbType2.
        // NOTE: INDEXED BY SqlDbType ENUM!  MUST UPDATE THIS ARRAY WHEN UPDATING SqlDbType!
        //   ONLY ACCESS THIS GLOBAL FROM GetDefaultForType!
        private static readonly SmiMetaData[] s_defaultValues =
            {
                DefaultBigInt,                 // SqlDbType2.BigInt
                DefaultBinary,                 // SqlDbType2.Binary
                DefaultBit,                    // SqlDbType2.Bit
                DefaultChar_NoCollation,       // SqlDbType2.Char
                DefaultDateTime,               // SqlDbType2.DateTime
                DefaultDecimal,                // SqlDbType2.Decimal
                DefaultFloat,                  // SqlDbType2.Float
                DefaultImage,                  // SqlDbType2.Image
                DefaultInt,                    // SqlDbType2.Int
                DefaultMoney,                  // SqlDbType2.Money
                DefaultNChar_NoCollation,      // SqlDbType2.NChar
                DefaultNText_NoCollation,      // SqlDbType2.NText
                DefaultNVarChar_NoCollation,   // SqlDbType2.NVarChar
                DefaultReal,                   // SqlDbType2.Real
                DefaultUniqueIdentifier,       // SqlDbType2.UniqueIdentifier
                DefaultSmallDateTime,          // SqlDbType2.SmallDateTime
                DefaultSmallInt,               // SqlDbType2.SmallInt
                DefaultSmallMoney,             // SqlDbType2.SmallMoney
                DefaultText_NoCollation,       // SqlDbType2.Text
                DefaultTimestamp,              // SqlDbType2.Timestamp
                DefaultTinyInt,                // SqlDbType2.TinyInt
                DefaultVarBinary,              // SqlDbType2.VarBinary
                DefaultVarChar_NoCollation,    // SqlDbType2.VarChar
                DefaultVariant,                // SqlDbType2.Variant
                DefaultNVarChar_NoCollation,   // Placeholder for value 24
                DefaultXml,                    // SqlDbType2.Xml
                DefaultNVarChar_NoCollation,   // Placeholder for value 26
                DefaultNVarChar_NoCollation,   // Placeholder for value 27
                DefaultNVarChar_NoCollation,   // Placeholder for value 28
                DefaultUdt_NoType,             // Generic Udt
                DefaultStructured,             // Generic structured type
                DefaultDate,                   // SqlDbType2.Date
                DefaultTime,                   // SqlDbType2.Time
                DefaultDateTime2,              // SqlDbType2.DateTime2
                DefaultDateTimeOffset,         // SqlDbType2.DateTimeOffset
            };

        // static array of type names ordered by corresponding SqlDbType2.
        // NOTE: INDEXED BY SqlDbType ENUM!  MUST UPDATE THIS ARRAY WHEN UPDATING SqlDbType!
        //   ONLY ACCESS THIS GLOBAL FROM get_TypeName!
        private static readonly string[] s_typeNameByDatabaseType =
            {
                "bigint",               // SqlDbType2.BigInt
                "binary",               // SqlDbType2.Binary
                "bit",                  // SqlDbType2.Bit
                "char",                 // SqlDbType2.Char
                "datetime",             // SqlDbType2.DateTime
                "decimal",              // SqlDbType2.Decimal
                "float",                // SqlDbType2.Float
                "image",                // SqlDbType2.Image
                "int",                  // SqlDbType2.Int
                "money",                // SqlDbType2.Money
                "nchar",                // SqlDbType2.NChar
                "ntext",                // SqlDbType2.NText
                "nvarchar",             // SqlDbType2.NVarChar
                "real",                 // SqlDbType2.Real
                "uniqueidentifier",     // SqlDbType2.UniqueIdentifier
                "smalldatetime",        // SqlDbType2.SmallDateTime
                "smallint",             // SqlDbType2.SmallInt
                "smallmoney",           // SqlDbType2.SmallMoney
                "text",                 // SqlDbType2.Text
                "timestamp",            // SqlDbType2.Timestamp
                "tinyint",              // SqlDbType2.TinyInt
                "varbinary",            // SqlDbType2.VarBinary
                "varchar",              // SqlDbType2.VarChar
                "sql_variant",          // SqlDbType2.Variant
                null,                   // placeholder for 24
                "xml",                  // SqlDbType2.Xml
                null,                   // placeholder for 26
                null,                   // placeholder for 27
                null,                   // placeholder for 28
                string.Empty,           // SqlDbType2.Udt  -- get type name from Type.FullName instead.
                string.Empty,           // Structured types have user-defined type names.
                "date",                 // SqlDbType2.Date
                "time",                 // SqlDbType2.Time
                "datetime2",            // SqlDbType2.DateTime2
                "datetimeoffset",       // SqlDbType2.DateTimeOffset
            };

        // Internal setter to be used by constructors only!  Modifies state!
        private void SetDefaultsForType(SqlDbType dbType)
        {
            SmiMetaData smdDflt = GetDefaultForType(dbType);
            _databaseType = dbType;
            _maxLength = smdDflt.MaxLength;
            _precision = smdDflt.Precision;
            _scale = smdDflt.Scale;
            _localeId = smdDflt.LocaleId;
            _compareOptions = smdDflt.CompareOptions;
            _clrType = null;
            _isMultiValued = smdDflt._isMultiValued;
            _fieldMetaData = smdDflt._fieldMetaData;            // This is ok due to immutability
            _extendedProperties = smdDflt._extendedProperties;  // This is ok due to immutability
        }

        internal string TraceString() => TraceString(0);

        virtual internal string TraceString(int indent)
        {
            string indentStr = new string(' ', indent);
            string fields = string.Empty;
            if (_fieldMetaData != null)
            {
                foreach (SmiMetaData fieldMd in _fieldMetaData)
                {
                    fields = string.Format(CultureInfo.InvariantCulture,"{0}{1}\n\t", fields, fieldMd.TraceString(indent + 5));
                }
            }

            string properties = string.Empty;
            if (_extendedProperties != null)
            {
                foreach (SmiMetaDataProperty property in _extendedProperties.Values)
                {
                    properties = string.Format(CultureInfo.InvariantCulture,"{0}{1}                   {2}\n\t", properties, indentStr, property.TraceString());
                }
            }

            return string.Format(CultureInfo.InvariantCulture, "\n\t"
                + "{0}            SqlDbType={1:g}\n\t"
                + "{0}            MaxLength={2:d}\n\t"
                + "{0}            Precision={3:d}\n\t"
                + "{0}                Scale={4:d}\n\t"
                + "{0}             LocaleId={5:x}\n\t"
                + "{0}       CompareOptions={6:g}\n\t"
                + "{0}                 Type={7}\n\t"
                + "{0}          MultiValued={8}\n\t"
                + "{0}               fields=\n\t{9}"
                + "{0}           properties=\n\t{10}",
                indentStr,
                SqlDbType,
                MaxLength,
                Precision,
                Scale,
                LocaleId,
                CompareOptions,
                Type?.ToString() ?? "<null>",
                IsMultiValued,
                fields,
                properties
            );

        }
    }

    // SmiExtendedMetaData
    //
    //  Adds server-specific type extension information to base metadata, but still portable across a specific server.
    //
    internal class SmiExtendedMetaData : SmiMetaData
    {
        private readonly string _name;           // context-dependent identifier, i.e. parameter name for parameters, column name for columns, etc.

        // three-part name for typed xml schema and for udt names
        private readonly string _typeSpecificNamePart1;
        private readonly string _typeSpecificNamePart2;
        private readonly string _typeSpecificNamePart3;

        internal SmiExtendedMetaData(
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
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                false,
                null,
                null,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3
            )
        {
        }

        // SMI V200 ctor.
        internal SmiExtendedMetaData(
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
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                null,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3
            )
        {
        }

        // SMI V220 ctor.
        internal SmiExtendedMetaData(
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
            string udtAssemblyQualifiedName,
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3
        )
            : base(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                udtAssemblyQualifiedName,
                isMultiValued,
                fieldMetaData,
                extendedProperties
            )
        {
            Debug.Assert(null == name || MaxNameLength >= name.Length, "Name is too long");

            _name = name;
            _typeSpecificNamePart1 = typeSpecificNamePart1;
            _typeSpecificNamePart2 = typeSpecificNamePart2;
            _typeSpecificNamePart3 = typeSpecificNamePart3;
        }

        internal string Name => _name;

        internal string TypeSpecificNamePart1 => _typeSpecificNamePart1;

        internal string TypeSpecificNamePart2 => _typeSpecificNamePart2;

        internal string TypeSpecificNamePart3 => _typeSpecificNamePart3;

        internal override string TraceString(int indent)
        {
            return string.Format(CultureInfo.InvariantCulture,
                "{2}                 Name={0}"
                + "{1}"
                + "{2}TypeSpecificNamePart1='{3}'\n\t"
                + "{2}TypeSpecificNamePart2='{4}'\n\t"
                + "{2}TypeSpecificNamePart3='{5}'\n\t",
                _name ?? "<null>",
                base.TraceString(indent),
                new string(' ', indent),
                TypeSpecificNamePart1 ?? "<null>",
                TypeSpecificNamePart2 ?? "<null>",
                TypeSpecificNamePart3 ?? "<null>"
            );
        }
    }

    // SmiParameterMetaData
    //
    //  MetaData class to send parameter definitions to server.
    //  Sealed because we don't need to derive from it yet.
    internal sealed class SmiParameterMetaData : SmiExtendedMetaData
    {
        private readonly ParameterDirection _direction;

        // SMI V200 ctor.
        internal SmiParameterMetaData(
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
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            ParameterDirection direction
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                null,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                direction
            )
        {
        }

        // SMI V220 ctor.
        internal SmiParameterMetaData(
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
            string udtAssemblyQualifiedName,
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            ParameterDirection direction
        )
            : base(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                udtAssemblyQualifiedName,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3
            )
        {
            Debug.Assert(ParameterDirection.Input == direction
                       || ParameterDirection.Output == direction
                       || ParameterDirection.InputOutput == direction
                       || ParameterDirection.ReturnValue == direction, "Invalid direction: " + direction);
            _direction = direction;
        }

        internal ParameterDirection Direction => _direction;

        internal override string TraceString(int indent)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}"
                + "{1}            Direction={2:g}\n\t",
                base.TraceString(indent),
                new string(' ', indent),
                Direction
            );
        }
    }

    // SmiStorageMetaData
    //
    //  This class represents the addition of storage-level attributes to the hierarchy (i.e. attributes from 
    //  underlying table, source variables, or whatever).
    //
    //  Most values use Null (either IsNullable == true or CLR null) to indicate "Not specified" state.  Selection
    //  of which values allow "not specified" determined by backward compatibility.
    //
    //  Maps approximately to TDS' COLMETADATA token with TABNAME and part of COLINFO thrown in.
    internal class SmiStorageMetaData : SmiExtendedMetaData
    {
        // AllowsDBNull is the only value required to be specified.
        private readonly bool _allowsDBNull;  // could the column return nulls? equivalent to TDS's IsNullable bit
        private readonly string _serverName;  // underlying column's server
        private readonly string _catalogName; // underlying column's database
        private readonly string _schemaName;  // underlying column's schema
        private readonly string _tableName;   // underlying column's table
        private readonly string _columnName;  // underlying column's name
        private readonly SqlBoolean _isKey;   // Is this one of a set of key columns that uniquely identify an underlying table?
        private readonly bool _isIdentity;    // Is this from an identity column
        private readonly bool _isColumnSet;   // Is this column the XML representation of a columnset?

        internal SmiStorageMetaData(
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
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                false,
                null,
                null,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                allowsDBNull,
                serverName,
                catalogName,
                schemaName,
                tableName,
                columnName,
                isKey,
                isIdentity
            )
        {
        }

        // SMI V200 ctor.
        internal SmiStorageMetaData(
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
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                null,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                allowsDBNull,
                serverName,
                catalogName,
                schemaName,
                tableName,
                columnName,
                isKey,
                isIdentity,
                false
            )
        {
        }

        // SMI V220 ctor.
        internal SmiStorageMetaData(
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
            string udtAssemblyQualifiedName,
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity,
            bool isColumnSet
        )
            : base(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                udtAssemblyQualifiedName,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3
             )
        {
            _allowsDBNull = allowsDBNull;
            _serverName = serverName;
            _catalogName = catalogName;
            _schemaName = schemaName;
            _tableName = tableName;
            _columnName = columnName;
            _isKey = isKey;
            _isIdentity = isIdentity;
            _isColumnSet = isColumnSet;
        }

        internal bool AllowsDBNull => _allowsDBNull;

        internal string ServerName => _serverName;

        internal string CatalogName => _catalogName;

        internal string SchemaName => _schemaName;

        internal string TableName => _tableName;

        internal string ColumnName => _columnName;

        internal SqlBoolean IsKey => _isKey;

        internal bool IsIdentity => _isIdentity;

        internal bool IsColumnSet => _isColumnSet;

        internal override string TraceString(int indent)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}"
                + "{1}         AllowsDBNull={2}\n\t"
                + "{1}           ServerName='{3}'\n\t"
                + "{1}          CatalogName='{4}'\n\t"
                + "{1}           SchemaName='{5}'\n\t"
                + "{1}            TableName='{6}'\n\t"
                + "{1}           ColumnName='{7}'\n\t"
                + "{1}                IsKey={8}\n\t"
                + "{1}           IsIdentity={9}\n\t",
                base.TraceString(indent),
                new string(' ', indent),
                AllowsDBNull,
                ServerName ?? "<null>",
                CatalogName ?? "<null>",
                SchemaName ?? "<null>",
                TableName ?? "<null>",
                ColumnName ?? "<null>",
                IsKey,
                IsIdentity
            );
        }
    }

    // SmiQueryMetaData
    //
    //  Adds Query-specific attributes.
    //  Sealed since we don't need to extend it for now.
    //  Maps to full COLMETADATA + COLINFO + TABNAME tokens on TDS.
    internal class SmiQueryMetaData : SmiStorageMetaData
    {
        private readonly bool _isReadOnly;
        private readonly SqlBoolean _isExpression;
        private readonly SqlBoolean _isAliased;
        private readonly SqlBoolean _isHidden;

        internal SmiQueryMetaData(
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
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity,
            bool isReadOnly,
            SqlBoolean isExpression,
            SqlBoolean isAliased,
            SqlBoolean isHidden
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                false,
                null,
                null,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                allowsDBNull,
                serverName,
                catalogName,
                schemaName,
                tableName,
                columnName,
                isKey,
                isIdentity,
                isReadOnly,
                isExpression,
                isAliased,
                isHidden
            )
        {
        }

        // SMI V200 ctor.
        internal SmiQueryMetaData(
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
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity,
            bool isReadOnly,
            SqlBoolean isExpression,
            SqlBoolean isAliased,
            SqlBoolean isHidden
        )
            : this(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                null,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                allowsDBNull,
                serverName,
                catalogName,
                schemaName,
                tableName,
                columnName,
                isKey,
                isIdentity,
                false,
                isReadOnly,
                isExpression,
                isAliased,
                isHidden
            )
        {
        }

        // SMI V220 ctor.
        internal SmiQueryMetaData(
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
            string udtAssemblyQualifiedName,
            bool isMultiValued,
            IList<SmiExtendedMetaData> fieldMetaData,
            SmiMetaDataPropertyCollection extendedProperties,
            string name,
            string typeSpecificNamePart1,
            string typeSpecificNamePart2,
            string typeSpecificNamePart3,
            bool allowsDBNull,
            string serverName,
            string catalogName,
            string schemaName,
            string tableName,
            string columnName,
            SqlBoolean isKey,
            bool isIdentity,
            bool isColumnSet,
            bool isReadOnly,
            SqlBoolean isExpression,
            SqlBoolean isAliased,
            SqlBoolean isHidden
        )
            : base(
                dbType,
                maxLength,
                precision,
                scale,
                localeId,
                compareOptions,
                userDefinedType,
                udtAssemblyQualifiedName,
                isMultiValued,
                fieldMetaData,
                extendedProperties,
                name,
                typeSpecificNamePart1,
                typeSpecificNamePart2,
                typeSpecificNamePart3,
                allowsDBNull,
                serverName,
                catalogName,
                schemaName,
                tableName,
                columnName,
                isKey,
                isIdentity,
                isColumnSet
            )
        {
            _isReadOnly = isReadOnly;
            _isExpression = isExpression;
            _isAliased = isAliased;
            _isHidden = isHidden;
        }

        internal bool IsReadOnly => _isReadOnly;

        internal SqlBoolean IsExpression => _isExpression;

        internal SqlBoolean IsAliased => _isAliased;

        internal SqlBoolean IsHidden => _isHidden;

        internal override string TraceString(int indent)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}"
                + "{1}           IsReadOnly={2}\n\t"
                + "{1}         IsExpression={3}\n\t"
                + "{1}            IsAliased={4}\n\t"
                + "{1}             IsHidden={5}",
                base.TraceString(indent),
                new string(' ', indent),
                AllowsDBNull,
                IsExpression,
                IsAliased,
                IsHidden
            );
        }

    }
}
