// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Data;

namespace Microsoft.Data
{
    /// <summary>
    /// Specifies SQL Server-specific data type of a field, property, for use in a <see cref="SqlClient.SqlParameter" />.
    /// </summary>
    public enum SqlDbType2
    {
        /// <summary>
        /// <see cref="long" />. A 64-bit signed integer.
        /// </summary>
        BigInt = SqlDbType.BigInt,

        /// <summary>
        /// <see cref="System.Array"/> of type <see cref="byte"/>. A fixed-length stream of binary data ranging between 1 and 8,000 bytes.
        /// </summary>
        Binary = SqlDbType.Binary,

        /// <summary>
        /// <see cref="bool"/>. An unsigned numeric value that can be 0, 1, or <see langword="null"/>
        /// </summary>
        Bit = SqlDbType.Bit,

        /// <summary>
        /// <see cref="string"/>. A fixed-length stream of non-Unicode characters ranging between 1 and 8,000 characters.
        /// </summary>
        Char = SqlDbType.Char,

        /// <summary>
        /// <see cref="System.DateTime"/>. Date and time data ranging in value from January 1, 1753 to December 31, 9999 to an accuracy of 3.33 milliseconds
        /// </summary>
        DateTime = SqlDbType.DateTime,

        /// <summary>
        /// <see cref="decimal"/>. A fixed precision and scale numeric value between -10 <sup>38</sup> -1 and 10 <sup>38</sup> -1.
        /// </summary>
        Decimal = SqlDbType.Decimal,

        /// <summary>
        /// <see cref="double"/>. A floating point number within the range of -1.79E +308 through 1.79E +308.
        /// </summary>
        Float = SqlDbType.Float,

        /// <summary>
        /// <see cref="System.Array"/> of type <see cref="byte"/>. A variable-length stream of binary data ranging from 0 to 2 <sup>31</sup> -1
        /// (or 2,147,483,647) bytes.
        /// </summary>
        Image = SqlDbType.Decimal,

        /// <summary>
        /// <see cref="int"/>. A 32-bit signed integer.
        /// </summary>
        Int = SqlDbType.Int,

        /// <summary>
        /// <see cref="decimal"/>. A currency value ranging from -2 <sup>63</sup> (or -9,223,372,036,854,775,808) to 2 <sup>63</sup>
        /// -1 (or +9,223,372,036,854,775,807) with an accuracy to a ten-thousandth of a currency unit.
        /// </summary>
        Money = SqlDbType.Money,

        /// <summary>
        /// <see cref="string"/>. A fixed-length stream of Unicode characters ranging between 1 and 4,000 characters.
        /// </summary>
        NChar = SqlDbType.NChar,

        /// <summary>
        /// <see cref="string"/>. A variable-length stream of Unicode data with a maximum length of 2 <sup>30</sup> - 1
        /// (or 1,073,741,823) characters.
        /// </summary>
        NText = SqlDbType.NText,

        /// <summary>
        /// <see cref="string"/>. A variable-length stream of Unicode characters ranging between 1 and 4,000 characters. Implicit
        /// conversion fails if the string is greater than 4,000 characters. Explicitly set the object when working with strings
        /// longer than 4,000 characters. Use <see cref="F:System.Data.SqlDbType.NVarChar"/> when the database column is
        /// <see langword="nvarchar(max)"/>.
        /// </summary>
        NVarChar = SqlDbType.NVarChar,

        /// <summary>
        /// <see cref="float"/>. A floating point number within the range of -3.40E +38 through 3.40E +38.
        /// </summary>
        Real = SqlDbType.Real,

        /// <summary>
        /// <see cref="System.Guid"/>. A globally unique identifier (or GUID).
        /// </summary>
        UniqueIdentifier = SqlDbType.UniqueIdentifier,

        /// <summary>
        /// <see cref="System.DateTime"/>. Date and time data ranging in value from January 1, 1900 to June 6, 2079 to an accuracy of one minute.
        /// </summary>
        SmallDateTime = SqlDbType.SmallDateTime,

        /// <summary>
        /// <see cref="short"/>. A 16-bit signed integer.
        /// </summary>
        SmallInt = SqlDbType.SmallInt,

        /// <summary>
        /// <see cref="decimal"/>. A currency value ranging from -214,748.3648 to +214,748.3647 with an accuracy to a ten-thousandth of a currency unit.
        /// </summary>
        SmallMoney =  SqlDbType.SmallMoney,

        /// <summary>
        /// <see cref="string"/>. A variable-length stream of non-Unicode data with a maximum length of 2 <sup>31</sup> -1 (or 2,147,483,647) characters.
        /// </summary>
        Text = SqlDbType.Text,

        /// <summary>
        /// <see cref="System.Array"/> of type <see cref="byte"/>. Automatically generated binary numbers, which are guaranteed to be unique within a database. <see langword="timestamp"/> is used typically as a mechanism for version-stamping table rows. The storage size is 8 bytes.
        /// </summary>
        Timestamp = SqlDbType.Timestamp,

        /// <summary>
        /// <see cref="byte"/>. An 8-bit unsigned integer.
        /// </summary>
        TinyInt = SqlDbType.TinyInt,

        /// <summary>
        /// <see cref="System.Array"/> of type <see cref="byte"/>. A variable-length stream of binary data ranging between 1 and 8,000 bytes.
        /// Implicit conversion fails if the byte array is greater than 8,000 bytes. Explicitly set the object when working with byte arrays
        /// larger than 8,000 bytes.
        /// </summary>
        VarBinary = SqlDbType.VarBinary,

        /// <summary>
        /// <see cref="string"/>. A variable-length stream of non-Unicode characters ranging between 1 and 8,000 characters.
        /// Use <see cref="F:System.Data.SqlDbType.VarChar"/> when the database column is <see langword="varchar(max)"/>.
        /// </summary>
        VarChar = SqlDbType.VarChar,

        /// <summary>
        /// <see cref="object"/>. A special data type that can contain numeric, string, binary, or date data as well as the SQL Server values Empty and Null,
        /// which is assumed if no other type is declared.
        /// </summary>
        Variant = SqlDbType.Variant,

        /// <summary>
        /// An XML value. Obtain the XML as a string using the <see cref="M:Microsoft.Data.SqlClient.SqlDataReader.GetValue(System.Int32)"/> method or
        /// <see cref="P:System.Data.SqlTypes.SqlXml.Value"/> property, or as an <see cref="T:System.Xml.XmlReader"/> by calling the
        /// <see cref="M:System.Data.SqlTypes.SqlXml.CreateReader"/> method.
        /// </summary>
        Xml = SqlDbType.Xml,

        /// <summary>
        /// A SQL Server user-defined type (UDT).
        /// </summary>
        Udt = SqlDbType.Udt,

        /// <summary>
        /// A special data type for specifying structured data contained in table-valued parameters.
        /// </summary>
        Structured = SqlDbType.Structured,

        /// <summary>
        /// Date data ranging in value from January 1,1 AD through December 31, 9999 AD.
        /// </summary>
        Date = SqlDbType.Date,

        /// <summary>
        /// Time data based on a 24-hour clock. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds.
        /// Corresponds to a SQL Server <see langword="time"/> value.
        /// </summary>
        Time = SqlDbType.Time,

        /// <summary>
        /// Date and time data. Date value range is from January 1,1 AD through December 31, 9999 AD.
        /// Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds.
        /// </summary>
        DateTime2 = SqlDbType.DateTime2,

        /// <summary>
        /// Date and time data with time zone awareness. Date value range is from January 1,1 AD through December 31, 9999 AD.
        /// Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds. Time zone value range
        /// is -14:00 through +14:00.
        /// </summary>
        DateTimeOffset = SqlDbType.DateTimeOffset,

        /// <summary>
        /// A Json Value.
        /// </summary>
        Json = 35,
    }
}
