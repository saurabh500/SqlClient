// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.Reflection;
using Microsoft.Data.SqlClient.Server;
using Xunit;

namespace Microsoft.Data.SqlClient.Tests
{
    public class SqlMetaDataTest
    {
        [Theory]
        [MemberData(nameof(SqlMetaDataAdjustValues))]
        [MemberData(nameof(SqlMetaDataDateTimeValues))]
        public void Adjust(SqlDbType dbType, object expected)
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                dbType,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);
            object actual = metaData.Adjust(expected);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [MemberData(nameof(SqlMetaDataMaxLengthTrimValues))]
        public void AdjustWithGreaterThanMaxLengthValues(SqlDbType dbType, object value)
        {
            int maxLength = 4;
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                dbType,
                maxLength,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);
            object actual = metaData.Adjust(value);
            Assert.NotEqual(value, actual);
        }

        [Theory]
        [MemberData(nameof(SqlMetaDataInvalidValues))]
        public void AdjustWithInvalidType_Throws(SqlDbType dbType, object expected)
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                dbType,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);
            ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() =>
            {
                object actual = metaData.Adjust(expected);
            });
            Assert.Contains("invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
        }


        [Fact]
        public void AdjustWithNullBytes()
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                SqlDbType2.Binary,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);

            byte[] array = null;
            object actual = metaData.Adjust(array);
            Assert.Null(actual);
        }

        [Fact]
        public void AdjustWithNullChars()
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                SqlDbType2.VarChar,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);

            char[] array = null;
            object actual = metaData.Adjust(array);
            Assert.Null(actual);
        }

        [Fact]
        public void AdjustWithNullString()
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                SqlDbType2.VarChar,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);
            string value = null;
            string ret = metaData.Adjust(value);
            Assert.Null(ret);
        }

        [Fact]
        public void AdjustWithOutOfRangeDateTime()
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                SqlDbType2.SmallDateTime,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);

            DateTime date = new DateTime(2080, 06, 06, 23, 59, 29, 999);
            ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() =>
            {
                object actual = metaData.Adjust(date);
            });
            Assert.Contains("invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void AdjustWithOutOfRangeTimeSpan_Throws()
        {
            SqlMetaData metaData = new SqlMetaData(
                "col1",
                SqlDbType2.Time,
                4,
                2,
                2,
                0,
                SqlCompareOptions.IgnoreCase,
                null,
                true,
                true,
                SortOrder.Ascending,
                0);

            TimeSpan outOfRangeTimespan = new TimeSpan(TimeSpan.TicksPerDay);
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                object actual = metaData.Adjust(outOfRangeTimespan);
            });
            Assert.Contains("invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void AdjustXml()
        {
            SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Xml, "NorthWindDb", "Schema", "ObjectName");
            SqlXml xml = metaData.Adjust(SqlXml.Null);
            Assert.True(xml.IsNull);
        }

        [Fact]
        public void ConstructorWithDefaultLocale()
        {
            SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.NText, true, true, SortOrder.Ascending, 0);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(CultureInfo.CurrentCulture.LCID, metaData.LocaleId);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Fact]
        public void ConstructorWithDefaultLocaleInvalidType_Throws()
        {
            SqlDbType invalidType = SqlDbType2.Structured;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", invalidType, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(SqlDbType2.Char)]
        [InlineData(SqlDbType2.VarChar)]
        [InlineData(SqlDbType2.NChar)]
        [InlineData(SqlDbType2.NVarChar)]
        public void ConstructorWithMaxLengthAndDefaultLocale(SqlDbType dbType)
        {
            const int maxLength = 5;
            SqlMetaData metaData = new SqlMetaData("col1", dbType, maxLength, true, true, SortOrder.Ascending, 0);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(CultureInfo.CurrentCulture.LCID, metaData.LocaleId);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Fact]
        public void ConstructorWithMaxLengthAndDefaultLocaleInvalidType_Throws()
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, 5, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(SqlDbType2.Char)]
        [InlineData(SqlDbType2.VarChar)]
        [InlineData(SqlDbType2.NChar)]
        [InlineData(SqlDbType2.NVarChar)]
        public void ConstructorWithMaxLengthAndLocale(SqlDbType dbType)
        {
            long maxLength = 5L;
            long locale = 0L;
            SqlMetaData metaData = new SqlMetaData("col1", dbType, maxLength, locale, SqlCompareOptions.IgnoreCase, true, true, SortOrder.Ascending, 0);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(locale, metaData.LocaleId);
            Assert.Equal(maxLength, metaData.MaxLength);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Fact]
        public void ConstructorWithMaxLengthAndLocaleInvalidType_Throws()
        {
            long locale = 0L;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, 5, locale, SqlCompareOptions.IgnoreCase, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(SqlDbType2.NText)]
        [InlineData(SqlDbType2.Text)]
        public void ConstructorWithMaxLengthTextAndDefaultLocale(SqlDbType dbType)
        {
            long maxLength = SqlMetaData.Max;
            SqlMetaData metaData = new SqlMetaData("col1", dbType, maxLength, true, true, SortOrder.Ascending, 0);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(CultureInfo.CurrentCulture.LCID, metaData.LocaleId);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Theory]
        [InlineData(SqlDbType2.NText)]
        [InlineData(SqlDbType2.Text)]
        public void ConstructorWithMaxLengthTextAndLocale(SqlDbType dbType)
        {
            long maxLength = SqlMetaData.Max;
            long locale = 0L;
            SqlMetaData metaData = new SqlMetaData("col1", dbType, maxLength, locale, SqlCompareOptions.IgnoreCase, true, true, SortOrder.Ascending, 0);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(locale, metaData.LocaleId);
            Assert.Equal(maxLength, metaData.MaxLength);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Theory]
        [InlineData(SqlDbType2.Char)]
        [InlineData(SqlDbType2.VarChar)]
        [InlineData(SqlDbType2.NChar)]
        [InlineData(SqlDbType2.NVarChar)]
        [InlineData(SqlDbType2.NText)]
        public void ConstructorWithInvalidMaxLengthAndLocale_Throws(SqlDbType dbType)
        {
            int invalidMaxLength = -2;
            long locale = 0L;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", dbType, invalidMaxLength, locale, SqlCompareOptions.IgnoreCase, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("out of range", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConstructorWithInvalidMaxLengthAndLocaleCompareOptionsBinarySortAndIgnoreCase_Throws()
        {
            long maxLength = SqlMetaData.Max;
            long locale = 0L;
            SqlCompareOptions invalidCompareOptions = SqlCompareOptions.BinarySort | SqlCompareOptions.IgnoreCase;
            ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.NText, maxLength, locale, invalidCompareOptions, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(SqlDbType2.Char)]
        [InlineData(SqlDbType2.VarChar)]
        [InlineData(SqlDbType2.NChar)]
        [InlineData(SqlDbType2.NVarChar)]
        [InlineData(SqlDbType2.NText)]
        [InlineData(SqlDbType2.Binary)]
        [InlineData(SqlDbType2.VarBinary)]
        [InlineData(SqlDbType2.Image)]
        public void ConstructorWithInvalidMaxLengthDefaultLocale_Throws(SqlDbType dbType)
        {
            int invalidMaxLength = -2;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", dbType, invalidMaxLength, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("out of range", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConstructorWithLongName_Throws()
        {
            string invalidName = new string('c', 256);

            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData(invalidName, SqlDbType2.Decimal, 2, 2, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("long", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConstructorWithNullName_Throws()
        {
            string invalidName = null;

            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData(invalidName, SqlDbType2.Decimal, 2, 2, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConstructorWithInvalidSortOrder_Throws()
        {
            SortOrder invalidSortOrder = (SortOrder)5;
            ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, true, true, invalidSortOrder, 0);
            });
            Assert.Contains("SortOrder", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ConstructorWithInvalidSortOrderSortOrdinal_Throws()
        {
            SortOrder invalidSortOrder = SortOrder.Unspecified;
            int invalidMatchToSortOrdinal = 0;
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, true, true, invalidSortOrder, invalidMatchToSortOrdinal);
            });
            Assert.Contains("sort order and ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void DbTypeDatabaseOwningSchemaObjectNameConstructorWithInvalidDbTypeName_Throws()
        {
            ArgumentException ex = Assert.ThrowsAny<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Int, "NorthWindDb", "schema", "name");
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void DecimalConstructor()
        {
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Decimal, 2, 2, true, true, SortOrder.Ascending, 1);
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.Decimal, metaData.SqlDbType);
            Assert.Null(metaData.Type);
            Assert.Equal(2, metaData.Precision);
            Assert.Equal(2, metaData.Scale);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(1, metaData.SortOrdinal);
        }

        [Fact]
        public void DecimalConstructorWithPrecisionScale()
        {
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Decimal, 2, 2);
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.Decimal, metaData.SqlDbType);
            Assert.Null(metaData.Type);
            Assert.Equal(2, metaData.Precision);
            Assert.Equal(2, metaData.Scale);
        }

        [Fact]
        public void DecimalConstructorWithNullUdt()
        {
            SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Decimal, 5, 2, 2, 0, SqlCompareOptions.BinarySort, null);
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(SqlDbType2.Decimal, metaData.SqlDbType);
            Assert.Equal(5, metaData.MaxLength);
            Assert.Equal(2, metaData.Precision);
            Assert.Equal(2, metaData.Scale);
            Assert.Equal(0, metaData.LocaleId);
            Assert.Null(metaData.Type);
        }

        [Fact]
        public void DecimalConstructorWithPrecisionOutOfRange_Throws()
        {
            byte precision = 1;
            byte scale = 2;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Decimal, precision, scale, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("precision", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void DecimalConstructorWithPrecisionOutofRange2_Throws()
        {
            byte precision = SqlDecimal.MaxPrecision;
            precision += 1;
            byte scale = 2;
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Decimal, precision, scale, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("precision", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // TODO: This condition can never be met because SqlDecimal.MaxPrecision == SqlDecimal.MaxScale
        // and there's a check that scale cannot exceed precision, so we cannot test this exception.
        //[Fact]
        //public void DecimalConstructorWithScaleOutOfRange_Throws()
        //{
        //    byte precision = SqlDecimal.MaxPrecision;
        //    byte scale = SqlDecimal.MaxScale;
        //    scale += 1;

        //    ArgumentException ex = Assert.Throws<ArgumentException>(() =>
        //    {
        //        SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Decimal, precision, scale, true, true, SortOrder.Ascending, 0);
        //    });
        //    Assert.NotNull(ex);
        //    Assert.NotEmpty(ex.Message);
        //    Assert.Contains("scale", ex.Message, StringComparison.OrdinalIgnoreCase);
        //}

        [Theory]
        [InlineData(SqlDbType2.Variant, null)]
        [InlineData(SqlDbType2.Udt, typeof(Address))]
        public void GenericConstructorWithoutXmlSchema(SqlDbType dbType, Type udt)
        {
            if (udt != null)
            {
                Type t = udt.GetInterface("IBinarySerialize", true);
                Assert.Equal(typeof(Microsoft.SqlServer.Server.IBinarySerialize), t);
            }
            SqlMetaData metaData = new SqlMetaData("col2", dbType, 16, 2, 2, 2, SqlCompareOptions.IgnoreCase, udt, true, true, SortOrder.Ascending, 0);
            Assert.Equal(dbType, metaData.SqlDbType);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Fact]
        public void GenericConstructorWithoutXmlSchemaWithInvalidDbType_Throws()
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col2", (SqlDbType)999, 16, 2, 2, 2, SqlCompareOptions.IgnoreCase, null, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void GetPartialLengthWithXmlSqlMetaDataType_Throws()
        {
            Type sqlMetaDataType = typeof(SqlMetaData);
            SqlMetaData exampleMetaData = new SqlMetaData("col2", SqlDbType2.Xml);
            MethodInfo method = sqlMetaDataType.GetMethod("GetPartialLengthMetaData", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Exception ex = Assert.ThrowsAny<Exception>(() =>
            {
                SqlMetaData metaData = (SqlMetaData)method.Invoke(exampleMetaData, new object[] { exampleMetaData });
            });
            Assert.NotNull(ex.InnerException);
            Assert.IsType<ArgumentException>(ex.InnerException);
            Assert.NotEmpty(ex.InnerException.Message);
            Assert.Contains("metadata", ex.InnerException.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(SqlDbType2.NVarChar)]
        [InlineData(SqlDbType2.VarChar)]
        [InlineData(SqlDbType2.VarBinary)]
        public void GetPartialLengthWithVarSqlMetaDataType(SqlDbType sqlDbType)
        {
            Type sqlMetaDataType = typeof(SqlMetaData);
            SqlMetaData exampleMetaData = new SqlMetaData("col2", sqlDbType, 16);
            MethodInfo method = sqlMetaDataType.GetMethod("GetPartialLengthMetaData", BindingFlags.NonPublic | BindingFlags.Static);
            SqlMetaData metaData = metaData = (SqlMetaData)method.Invoke(exampleMetaData, new object[] { exampleMetaData });
            Assert.Equal(exampleMetaData.Name, metaData.Name);
            Assert.Equal(exampleMetaData.SqlDbType, metaData.SqlDbType);
            Assert.Equal(SqlMetaData.Max, metaData.MaxLength);
            Assert.Equal(0, metaData.Precision);
            Assert.Equal(0, metaData.Scale);
            Assert.Equal(exampleMetaData.LocaleId, metaData.LocaleId);
            Assert.Equal(exampleMetaData.CompareOptions, metaData.CompareOptions);
            Assert.Null(metaData.XmlSchemaCollectionDatabase);
            Assert.Null(metaData.XmlSchemaCollectionName);
            Assert.Null(metaData.XmlSchemaCollectionOwningSchema);
            // PartialLength is an interal property which is why reflection is required.
            PropertyInfo isPartialLengthProp = sqlMetaDataType.GetProperty("IsPartialLength", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.True((bool)isPartialLengthProp.GetValue(metaData));
            Assert.Equal(exampleMetaData.Type, metaData.Type);
        }

        [Theory]
        [MemberData(nameof(SqlMetaDataInferredValues))]
        public void InferFromValue(SqlDbType expectedDbType, object value)
        {
            SqlMetaData metaData = SqlMetaData.InferFromValue(value, "col1");
            Assert.Equal(expectedDbType, metaData.SqlDbType);
        }

        [Theory]
        [InlineData((SByte)1)]
        [InlineData((UInt16)1)]
        [InlineData((UInt32)1)]
        [InlineData((UInt64)1)]
        public void InferFromValueWithInvalidValue_Throws(object value)
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = SqlMetaData.InferFromValue(value, "col1");
            });
            Assert.NotNull(ex);
            Assert.NotEmpty(ex.Message);
            Assert.Contains("invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void InferFromValueWithNull_Throws()
        {
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
            {
                SqlMetaData metaData = SqlMetaData.InferFromValue(null, "col1");
            });
            Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void InferFromValueWithUdtValue_Throws()
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                Address address = new Address();
                SqlMetaData metaData = SqlMetaData.InferFromValue(address, "col1");
            });
            Assert.Contains("address", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void IsPartialLengthTrueGetPartialLengthMetaData()
        {
            Type sqlMetaDataType = typeof(SqlMetaData);
            SqlMetaData exampleMetaData = new SqlMetaData("col2", SqlDbType2.Int);
            FieldInfo isPartialLengthField = sqlMetaDataType.GetField("_partialLength", BindingFlags.NonPublic | BindingFlags.Instance);
            isPartialLengthField.SetValue(exampleMetaData, true);
            MethodInfo method = sqlMetaDataType.GetMethod("GetPartialLengthMetaData", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            SqlMetaData metaData =  (SqlMetaData)method.Invoke(exampleMetaData, new object[] { exampleMetaData });
            Assert.Equal(exampleMetaData, metaData);
        }

        [Fact]
        public void NameDbTypeDatabaseOwningSchemaObjectNameConstructor()
        {
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Xml, "NorthWindDb", "schema", "name");
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.Xml, metaData.SqlDbType);
            Assert.Equal("NorthWindDb", metaData.XmlSchemaCollectionDatabase);
            Assert.Equal("schema", metaData.XmlSchemaCollectionOwningSchema);
            Assert.Equal("name", metaData.XmlSchemaCollectionName);
            Assert.Equal("xml", metaData.TypeName);
        }

        [Fact]
        public void NonVarTypeGetPartialLengthMetaData()
        {
            Type sqlMetaDataType = typeof(SqlMetaData);
            SqlMetaData exampleMetaData = new SqlMetaData("col2", SqlDbType2.Int);
            MethodInfo method = sqlMetaDataType.GetMethod("GetPartialLengthMetaData", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            SqlMetaData metaData = (SqlMetaData)method.Invoke(exampleMetaData, new object[] { exampleMetaData });
            Assert.Equal(exampleMetaData, metaData);
        }

        [Fact]
        public void StringConstructorWithLocaleCompareOption()
        {
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.VarChar, 16, 2, SqlCompareOptions.IgnoreCase, true, true, SortOrder.Ascending, 1);
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.VarChar, metaData.SqlDbType);
            Assert.Equal(DbType.AnsiString, metaData.DbType);
            Assert.Null(metaData.Type);
            Assert.Equal(16, metaData.MaxLength);
            Assert.Equal(2, metaData.LocaleId);
            Assert.Equal(SqlCompareOptions.IgnoreCase, metaData.CompareOptions);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(1, metaData.SortOrdinal);
        }

        [Theory]
        [InlineData(SqlDbType2.Time)]
        [InlineData(SqlDbType2.DateTime2)]
        [InlineData(SqlDbType2.DateTimeOffset)]
        public void TimeConstructorWithOutOfRange_Throws(SqlDbType dbType)
        {
            byte precision = 8;
            byte scale = 8;

            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", dbType, 5, precision, scale, 0, SqlCompareOptions.BinarySort, null, true, true, SortOrder.Ascending, 0);
            });
            Assert.Contains("scale", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void TimeConstructorWithInvalidType_Throws()
        {
            byte precision = 2;
            byte scale = 2;

            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, precision, scale);
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // Test UDT constrtuctor without tvp extended properties
        [Fact]
        public void UdtConstructorTest()
        {
            Address address = Address.Parse("123 baker st || Redmond");
            SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Udt, typeof(Address), "UdtTestDb.dbo.Address");
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(SqlDbType2.Udt, metaData.SqlDbType);
            Assert.Equal(address.GetType(), metaData.Type);
            Assert.Equal("UdtTestDb.dbo.Address", metaData.TypeName);
            Assert.False(metaData.UseServerDefault);
            Assert.False(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Unspecified, metaData.SortOrder);
            Assert.Equal(-1, metaData.SortOrdinal);
        }

        [Fact]
        public static void InvalidUdtEcxeption_Throws()
        {
            SqlServer.Server.InvalidUdtException e = 
                Assert.Throws<SqlServer.Server.InvalidUdtException> (() => new SqlMetaData("col1", SqlDbType2.Udt, typeof(int), "UdtTestDb.dbo.Address"));

            Assert.Equal("'System.Int32' is an invalid user defined type, reason: no UDT attribute.", e.Message);
        }

        [Fact]
        public void UdtConstructorTestWithoutServerTypeName()
        {
            Address address = Address.Parse("123 baker st || Redmond");
            SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Udt, typeof(Address));
            Assert.Equal("col1", metaData.Name);
            Assert.Equal(SqlDbType2.Udt, metaData.SqlDbType);
            Assert.Equal(address.GetType(), metaData.Type);
            Assert.Equal("Address", metaData.TypeName);
            Assert.False(metaData.UseServerDefault);
            Assert.False(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Unspecified, metaData.SortOrder);
            Assert.Equal(-1, metaData.SortOrdinal);
        }

        // Test UDT constrtuctor with tvp extended properties
        [Fact]
        public void UdtConstructorWithTvpTest()
        {
            Address address = Address.Parse("123 baker st || Redmond");
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Udt, typeof(Address), "UdtTestDb.dbo.Address", true, true, SortOrder.Ascending, 0);
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.Udt, metaData.SqlDbType);
            Assert.Equal(address.GetType(), metaData.Type);
            Assert.Equal("UdtTestDb.dbo.Address", metaData.TypeName);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(0, metaData.SortOrdinal);
        }

        [Fact]
        public void UdtConstructorWithInvalidType_Throws()
        {
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Int, typeof(int));
            });
            Assert.Contains("dbType", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void UdtConstructorWithNull_Throws()
        {
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Udt, null);
            });
            Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void XmlConstructor()
        {
            SqlMetaData metaData = new SqlMetaData("col2", SqlDbType2.Xml, "NorthWindDb", "schema", "name", true, true, SortOrder.Ascending, 1);
            Assert.Equal("col2", metaData.Name);
            Assert.Equal(SqlDbType2.Xml, metaData.SqlDbType);
            Assert.Null(metaData.Type);
            Assert.Equal("NorthWindDb", metaData.XmlSchemaCollectionDatabase);
            Assert.Equal("schema", metaData.XmlSchemaCollectionOwningSchema);
            Assert.Equal("name", metaData.XmlSchemaCollectionName);
            Assert.True(metaData.UseServerDefault);
            Assert.True(metaData.IsUniqueKey);
            Assert.Equal(SortOrder.Ascending, metaData.SortOrder);
            Assert.Equal(1, metaData.SortOrdinal);
        }

        [Fact]
        public void XmlConstructorWithNullObjectName_Throws()
        {
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
            {
                SqlMetaData metaData = new SqlMetaData("col1", SqlDbType2.Xml, "NorthWindDb", "schema", null);
            });
            Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        #region Test values
        public static readonly object[][] SqlMetaDataDateTimeValues =
        {
            new object[] {SqlDbType2.DateTime, new SqlDateTime(DateTime.UtcNow)},
            new object[] {SqlDbType2.Date, DateTime.Today},
            new object[] {SqlDbType2.DateTime, DateTime.Today},
            new object[] {SqlDbType2.DateTime2, DateTime.Today},
            new object[] {SqlDbType2.SmallDateTime, DateTime.Today},
        };

        public static readonly object[][] SqlMetaDataMaxLengthTrimValues =
        {
            new object[] {SqlDbType2.Binary, new SqlBinary(new byte[] { 1, 2, 3, 4, 5 })},
            new object[] {SqlDbType2.Binary, new byte[] { 1, 2, 3, 4, 5 }},
            new object[] {SqlDbType2.Char, "Tests"},
            new object[] {SqlDbType2.Char, "T"},
            new object[] {SqlDbType2.Char, new char[]{'T','e','s','t','s'}},
            new object[] {SqlDbType2.NChar, "T"},
            new object[] {SqlDbType2.NChar, "Tests"},
            new object[] {SqlDbType2.VarChar, "Tests" },
            new object[] {SqlDbType2.VarChar, new SqlString("Tests")},
            new object[] {SqlDbType2.VarChar, new char[]{'T','e','s','t','s'}},
            new object[] {SqlDbType2.NVarChar, "Tests"},
            new object[] {SqlDbType2.Binary, new SqlBytes(new byte[] { 1 })},
            new object[] {SqlDbType2.Binary, new byte[] { 1 }},
            new object[] {SqlDbType2.Timestamp, new SqlBytes(new byte[] { 1 })},
        };

        public static readonly object[][] SqlMetaDataInvalidValues =
        {
            new object[] {SqlDbType2.Char, 'T'},
            new object[] {SqlDbType2.NChar, 'T'},
            new object[] {SqlDbType2.Text, 'T'},
            new object[] {SqlDbType2.NText, 'T'},
            new object[] {SqlDbType2.Date, SqlDateTime.Null},
            new object[] {SqlDbType2.SmallInt, 1},
            new object[] {SqlDbType2.VarChar, SqlInt32.Zero},
            new object[] {SqlDbType2.BigInt, (short)1},
            new object[] {SqlDbType2.NVarChar, SqlInt16.Zero},
            new object[] {SqlDbType2.Text, 10L},
            new object[] {SqlDbType2.Binary, SqlInt64.Zero},
            new object[] {SqlDbType2.Float, 1.0f},
            new object[] {SqlDbType2.NChar, SqlSingle.Zero},
            new object[] {SqlDbType2.Timestamp, 1.0d},
            new object[] {SqlDbType2.Real, SqlDouble.Zero},
            new object[] {SqlDbType2.VarBinary, false},
            new object[] {SqlDbType2.NText, SqlBoolean.False},
            new object[] {SqlDbType2.Time, (byte)1},
            new object[] {SqlDbType2.Bit, SqlByte.Zero},
            new object[] {SqlDbType2.Decimal, SqlMoney.Zero},
            new object[] {SqlDbType2.SmallMoney, SqlDecimal.Null},
            new object[] {SqlDbType2.Money, SqlDecimal.Null},
            new object[] {SqlDbType2.Bit, SqlString.Null},
            new object[] {SqlDbType2.Int, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.Timestamp, new SqlString("T")},
            new object[] {SqlDbType2.Image, SqlChars.Null},
            new object[] {SqlDbType2.Int, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.Float, SqlBinary.Null},
            new object[] {SqlDbType2.Float, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.Float, SqlBytes.Null},
            new object[] {SqlDbType2.Float, new SqlBytes(new byte[] { 1, 0, 0, 0 })},
            new object[] {SqlDbType2.Float, new byte[] { 1, 0, 0, 0 }},
            new object[] {SqlDbType2.TinyInt, new SqlBytes(new byte[] { 1 })},
            new object[] {SqlDbType2.Bit, SqlBinary.Null},
            new object[] {SqlDbType2.Bit, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.Decimal, new SqlBytes()},
            new object[] {SqlDbType2.Char, new TimeSpan(0, 0, 1)},
            new object[] {SqlDbType2.UniqueIdentifier, new DateTimeOffset(new DateTime(0), TimeSpan.Zero)},
            new object[] {SqlDbType2.DateTimeOffset, SqlGuid.Null},
            new object[] {SqlDbType2.Date, new SqlDateTime(DateTime.UtcNow)},
            new object[] {SqlDbType2.Bit, SqlXml.Null },
            new object[] {SqlDbType2.Bit, (sbyte)0},
            new object[] {SqlDbType2.Bit, (UInt16)1},
            new object[] {SqlDbType2.Bit, (UInt32)1},
            new object[] {SqlDbType2.Bit, (UInt64)1},
            new object[] {SqlDbType2.Bit, (sbyte)0},
            new object[] {SqlDbType2.Int, Guid.Empty},
            new object[] {SqlDbType2.NText, 'T'},
            new object[] {SqlDbType2.SmallMoney, (decimal)int.MaxValue},
            new object[] {SqlDbType2.SmallMoney, "Money" },
            new object[] {SqlDbType2.Bit, 1.0M },
            new object[] {SqlDbType2.Bit, DateTime.Today},
        };

        public static readonly object[][] SqlMetaDataAdjustValues =
        {
            new object[] {SqlDbType2.Int, null},
            new object[] {SqlDbType2.Int, 1},
            new object[] {SqlDbType2.Int, SqlInt32.Zero},
            new object[] {SqlDbType2.SmallInt, (short)1},
            new object[] {SqlDbType2.SmallInt, SqlInt16.Zero},
            new object[] {SqlDbType2.BigInt, 10L},
            new object[] {SqlDbType2.BigInt, SqlInt64.Zero},
            new object[] {SqlDbType2.Real, 1.0f},
            new object[] {SqlDbType2.Real, SqlSingle.Zero},
            new object[] {SqlDbType2.Float, 1.0d},
            new object[] {SqlDbType2.Float, SqlDouble.Zero},
            new object[] {SqlDbType2.Bit, false},
            new object[] {SqlDbType2.Bit, SqlBoolean.False},
            new object[] {SqlDbType2.TinyInt, (byte)1},
            new object[] {SqlDbType2.TinyInt, SqlByte.Zero},
            new object[] {SqlDbType2.Money, 10.01M },
            new object[] {SqlDbType2.Money, SqlMoney.Zero},
            new object[] {SqlDbType2.SmallMoney, SqlMoney.Zero},
            new object[] {SqlDbType2.SmallMoney, 10.01M },
            new object[] {SqlDbType2.Decimal, 0M },
            new object[] {SqlDbType2.Decimal, SqlDecimal.Null},
            new object[] {SqlDbType2.Char, SqlString.Null},
            new object[] {SqlDbType2.Char, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.Char, "Test"},
            new object[] {SqlDbType2.Char, new SqlString("T")},
            new object[] {SqlDbType2.Char, new SqlString("Test")},
            new object[] {SqlDbType2.Char, SqlChars.Null},
            new object[] {SqlDbType2.Char, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.NChar, SqlString.Null},
            new object[] {SqlDbType2.NChar, new char[] {'T','e' ,'s', 't'}},
            new object[] {SqlDbType2.NChar, SqlChars.Null},
            new object[] {SqlDbType2.NChar, "Test"},
            new object[] {SqlDbType2.NChar, new SqlString("T")},
            new object[] {SqlDbType2.NChar, new SqlString("Test")},
            new object[] {SqlDbType2.NChar, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.VarChar, 'T'},
            new object[] {SqlDbType2.VarChar, "T"},
            new object[] {SqlDbType2.VarChar, "Test"},
            new object[] {SqlDbType2.VarChar, new SqlString("T")},
            new object[] {SqlDbType2.VarChar, new SqlString("Test")},
            new object[] {SqlDbType2.VarChar, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.NVarChar, 'T'},
            new object[] {SqlDbType2.NVarChar, "T"},
            new object[] {SqlDbType2.NVarChar, "Test"},
            new object[] {SqlDbType2.NVarChar, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.NVarChar, new SqlString("T")},
            new object[] {SqlDbType2.NVarChar, new SqlString("Test")},
            new object[] {SqlDbType2.NVarChar, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.NText, "T"},
            new object[] {SqlDbType2.NText, "Test"},
            new object[] {SqlDbType2.NText, "Tests"},
            new object[] {SqlDbType2.NText, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.NText, new SqlString("T")},
            new object[] {SqlDbType2.NText, new SqlString("Test")},
            new object[] {SqlDbType2.NText, new SqlString(new string('T', 17))},
            new object[] {SqlDbType2.NText, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.NText, new SqlChars(new char[] { 'T', 'e', '.', 't' })},
            new object[] {SqlDbType2.Text, "Tests"},
            new object[] {SqlDbType2.Text, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.Text, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.Binary, SqlBinary.Null},
            new object[] {SqlDbType2.Binary, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.Binary, SqlBytes.Null},
            new object[] {SqlDbType2.Binary, new SqlBytes(new byte[] { 1, 0, 0, 0 })},
            new object[] {SqlDbType2.Binary, new byte[] { 1, 0, 0, 0 }},
            new object[] {SqlDbType2.Binary, new SqlBytes(new byte[] { 1, 2, 3, 4, 5 })},
            new object[] {SqlDbType2.VarBinary, new SqlBytes(new byte[] { 1 })},
            new object[] {SqlDbType2.Timestamp, SqlBinary.Null},
            new object[] {SqlDbType2.Timestamp, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.Timestamp, new SqlBinary(new byte[] { 1, 2, 3, 4, 5 })},
            new object[] {SqlDbType2.Timestamp, new SqlBytes()},
            new object[] {SqlDbType2.Image, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.Image, new SqlBytes(new byte[] { 1 })},
            new object[] {SqlDbType2.Time, new TimeSpan(0, 0, 1)},
            new object[] {SqlDbType2.DateTimeOffset, new DateTimeOffset(new DateTime(0), TimeSpan.Zero)},
            new object[] {SqlDbType2.UniqueIdentifier, SqlGuid.Null},
            new object[] {SqlDbType2.UniqueIdentifier, Guid.Empty},
        };

        public static readonly object[][] SqlMetaDataInferredValues =
        {
            new object[] {SqlDbType2.Int, 1},
            new object[] {SqlDbType2.Int, SqlInt32.Zero},
            new object[] {SqlDbType2.SmallInt, (short)1},
            new object[] {SqlDbType2.SmallInt, SqlInt16.Zero},
            new object[] {SqlDbType2.BigInt, 10L},
            new object[] {SqlDbType2.BigInt, SqlInt64.Zero},
            new object[] {SqlDbType2.Real, 1.0f},
            new object[] {SqlDbType2.Real, SqlSingle.Zero},
            new object[] {SqlDbType2.Float, 1.0d},
            new object[] {SqlDbType2.Float, SqlDouble.Zero},
            new object[] {SqlDbType2.Bit, false},
            new object[] {SqlDbType2.Bit, SqlBoolean.False},
            new object[] {SqlDbType2.TinyInt, (byte)1},
            new object[] {SqlDbType2.TinyInt, SqlByte.Zero},
            new object[] {SqlDbType2.Money, SqlMoney.Zero},
            new object[] {SqlDbType2.Decimal, SqlDecimal.Null},
            new object[] {SqlDbType2.Decimal, new SqlDecimal(10.01M) },
            new object[] {SqlDbType2.Decimal, 10.01M },
            new object[] {SqlDbType2.NVarChar, "" },
            new object[] {SqlDbType2.NVarChar, 'T'},
            new object[] {SqlDbType2.NVarChar, "T"},
            new object[] {SqlDbType2.NVarChar, "Test"},
            new object[] {SqlDbType2.NVarChar, new string('a', 4001)},
            new object[] {SqlDbType2.NVarChar, new char[] {}},
            new object[] {SqlDbType2.NVarChar, new char[] {'T','e','s', 't'}},
            new object[] {SqlDbType2.NVarChar, new char[4001]},
            new object[] {SqlDbType2.NVarChar, new SqlString("T")},
            new object[] {SqlDbType2.NVarChar, new SqlString("Test")},
            new object[] {SqlDbType2.NVarChar, new SqlString("")},
            new object[] {SqlDbType2.NVarChar, new SqlString(new string('a', 4001))},
            new object[] {SqlDbType2.NVarChar, SqlString.Null},
            new object[] {SqlDbType2.NVarChar, new SqlChars(new char[] { 'T' })},
            new object[] {SqlDbType2.NVarChar, new SqlChars(new char[] { 'T', 'e', 's', 't' })},
            new object[] {SqlDbType2.NVarChar, new SqlChars(new char[] {})},
            new object[] {SqlDbType2.NVarChar, new SqlChars(new char[4001])},
            new object[] {SqlDbType2.NVarChar, SqlChars.Null},
            new object[] {SqlDbType2.VarBinary, new SqlBytes(new byte[] { })},
            new object[] {SqlDbType2.VarBinary, new SqlBytes(new byte[] { 1 })},
            new object[] {SqlDbType2.VarBinary, new SqlBytes(new byte[8001])},
            new object[] {SqlDbType2.VarBinary, SqlBytes.Null},
            new object[] {SqlDbType2.VarBinary, SqlBinary.Null},
            new object[] {SqlDbType2.VarBinary, new SqlBinary(new byte[] { })},
            new object[] {SqlDbType2.VarBinary, new SqlBinary(new byte[] { 1 })},
            new object[] {SqlDbType2.VarBinary, new SqlBinary(new byte[8001])},
            new object[] {SqlDbType2.VarBinary, new byte[] { }},
            new object[] {SqlDbType2.VarBinary, new byte[] { 1 }},
            new object[] {SqlDbType2.VarBinary, new byte[8001]},
            new object[] {SqlDbType2.Time, new TimeSpan(0, 0, 1)},
            new object[] {SqlDbType2.Time, new TimeSpan(TimeSpan.TicksPerDay - 1)},
            new object[] {SqlDbType2.DateTimeOffset, new DateTimeOffset(new DateTime(0), TimeSpan.Zero)},
            new object[] {SqlDbType2.DateTimeOffset, new DateTimeOffset(DateTime.Now)},
            new object[] {SqlDbType2.UniqueIdentifier, SqlGuid.Null},
            new object[] {SqlDbType2.UniqueIdentifier, Guid.Empty},
            new object[] {SqlDbType2.DateTime, new SqlDateTime(DateTime.UtcNow)},
            new object[] {SqlDbType2.DateTime, DateTime.Today},
            new object[] {SqlDbType2.Xml, new SqlXml()},
            new object[] {SqlDbType2.Variant, new object()}
        };
        #endregion
    }
}
