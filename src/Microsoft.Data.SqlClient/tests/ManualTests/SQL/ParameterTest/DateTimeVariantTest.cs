﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Data;
using Microsoft.Data.SqlClient.Server;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests
{
    public static class DateTimeVariantTest
    {
        private static string s_connStr;

        /// <summary>
        /// Tests all 2008 DateTime types inside sql_variant to server using sql_variant parameter, SqlBulkCopy, and TVP parameter with sql_variant inside.
        /// </summary>
        public static void TestAllDateTimeWithDataTypeAndVariant(string connStr)
        {
            s_connStr = connStr;

            SendInfo(System.DateTime.MinValue, "System.DateTime", "date");
            SendInfo(System.DateTime.MaxValue, "System.DateTime", "date");

            SendInfo(System.DateTime.MinValue, "System.DateTime", "datetime2");
            SendInfo(System.DateTime.MaxValue, "System.DateTime", "datetime2");

            SendInfo(System.DateTime.MinValue, "System.DateTime", "datetime");
            SendInfo(System.DateTime.MaxValue, "System.DateTime", "datetime");

            SendInfo(System.DateTimeOffset.MinValue, "System.DateTimeOffset", "datetimeoffset");
            SendInfo(System.DateTimeOffset.MaxValue, "System.DateTimeOffset", "datetimeoffset");

            SendInfo(System.DateTimeOffset.Parse("12/31/1999 23:59:59.9999999 -08:30"), "System.DateTimeOffset", "datetimeoffset");
            SendInfo(System.DateTime.Parse("1998-01-01 23:59:59.995"), "System.DateTime", "datetime2");

            SendInfo(System.DateTime.MinValue, "System.DateTime", "smalldatetime");
            SendInfo(System.DateTime.MaxValue, "System.DateTime", "smalldatetime");

            SendInfo(System.TimeSpan.MinValue, "System.TimeSpan", "time");
            SendInfo(System.TimeSpan.MaxValue, "System.TimeSpan", "time");

            SendInfo(System.DateTime.MinValue, "System.DateTime", "time");
            SendInfo(System.DateTime.MaxValue, "System.DateTime", "time");
        }

        private static void SendInfo(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            TestSimpleParameter_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            TestSimpleParameter_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            TestSqlDataRecordParameterToTVP_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            TestSqlDataRecordParameterToTVP_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            TestSqlDataReaderParameterToTVP_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            TestSqlDataReaderParameterToTVP_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            TestSqlDataReader_TVP_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            TestSqlDataReader_TVP_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            TestSimpleDataReader_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            TestSimpleDataReader_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            SqlBulkCopySqlDataReader_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            SqlBulkCopySqlDataReader_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            SqlBulkCopyDataTable_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            SqlBulkCopyDataTable_Variant(paramValue, expectedTypeName, expectedBaseTypeName);

            SqlBulkCopyDataRow_Type(paramValue, expectedTypeName, expectedBaseTypeName);
            SqlBulkCopyDataRow_Variant(paramValue, expectedTypeName, expectedBaseTypeName);
        }

        private static void TestSimpleParameter_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSimpleParameter_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string procName = DataTestUtility.GetUniqueNameForSqlServer("paramProc1");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
                xsql(conn, string.Format("create proc {0} (@param {1}) as begin select @param end;", procName, expectedBaseTypeName));

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter p = cmd.Parameters.AddWithValue("@param", paramValue);
                cmd.Parameters[0].SqlDbType = GetSqlDbType(expectedBaseTypeName);
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test Simple Parameter [Data Type]", expectedBaseTypeName, expectedTypeName, dr[0], expectedTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void TestSimpleParameter_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSimpleParameter_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string procName = DataTestUtility.GetUniqueNameForSqlServer("paramProc2");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
                xsql(conn, string.Format("create proc {0} (@param sql_variant) as begin select @param, sql_variant_property(@param,'BaseType') as BaseType end;", procName));

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter p = cmd.Parameters.AddWithValue("@param", paramValue);
                cmd.Parameters[0].SqlDbType = SqlDbType2.Variant;
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test Simple Parameter [Variant Type]", "SqlDbType2.Variant", dr, expectedTypeName, expectedBaseTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
            }
        }

        private static void TestSqlDataRecordParameterToTVP_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataRecordParameterToTVP_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpType");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
                xsql(conn, string.Format("create type dbo.{0} as table (f1 {1})", tvpTypeName, expectedBaseTypeName));

                // Send TVP using SqlMetaData.
                SqlMetaData[] metadata = new SqlMetaData[1];
                metadata[0] = new SqlMetaData("f1", GetSqlDbType(expectedBaseTypeName));
                SqlDataRecord[] record = new SqlDataRecord[1];
                record[0] = new SqlDataRecord(metadata);
                record[0].SetValue(0, paramValue);

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select f1 from @tvpParam";
                SqlParameter p = cmd.Parameters.AddWithValue("@tvpParam", record);
                p.SqlDbType = SqlDbType2.Structured;
                p.TypeName = string.Format("dbo.{0}", tvpTypeName);
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test SqlDataRecord Parameter To TVP [Data Type]", expectedBaseTypeName, expectedTypeName, dr[0], expectedTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void TestSqlDataRecordParameterToTVP_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataRecordParameterToTVP_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpVariant");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
                xsql(conn, string.Format("create type dbo.{0} as table (f1 sql_variant)", tvpTypeName));

                // Send TVP using SqlMetaData.
                SqlMetaData[] metadata = new SqlMetaData[1];
                metadata[0] = new SqlMetaData("f1", SqlDbType2.Variant);
                SqlDataRecord[] record = new SqlDataRecord[1];
                record[0] = new SqlDataRecord(metadata);
                record[0].SetValue(0, paramValue);

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select f1, sql_variant_property(f1,'BaseType') as BaseType from @tvpParam";
                SqlParameter p = cmd.Parameters.AddWithValue("@tvpParam", record);
                p.SqlDbType = SqlDbType2.Structured;
                p.TypeName = string.Format("dbo.{0}", tvpTypeName);
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test SqlDataRecord Parameter To TVP [Variant Type]", "SqlDbType2.Variant", dr, expectedTypeName, expectedBaseTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
            }
        }

        private static void TestSqlDataReaderParameterToTVP_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataReaderParameterToTVP_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpType");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
                xsql(conn, string.Format("create type dbo.{0} as table (f1 {1})", tvpTypeName, expectedBaseTypeName));
                using (SqlConnection connInput = new(s_connStr))
                {
                    connInput.Open();
                    using (SqlCommand cmdInput = connInput.CreateCommand())
                    {
                        cmdInput.CommandText = "select @p1 as f1";
                        cmdInput.Parameters.Add("@p1", GetSqlDbType(expectedBaseTypeName));
                        cmdInput.Parameters["@p1"].Value = paramValue;
                        using SqlDataReader drInput = cmdInput.ExecuteReader(CommandBehavior.CloseConnection);
                        using SqlCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select f1 from @tvpParam";
                        SqlParameter p = cmd.Parameters.AddWithValue("@tvpParam", drInput);
                        p.SqlDbType = SqlDbType2.Structured;
                        p.TypeName = string.Format("dbo.{0}", tvpTypeName);
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            dr.Read();
                            VerifyReaderTypeAndValue("Test SqlDataReader Parameter To TVP [Data Type]", expectedBaseTypeName, expectedTypeName, dr[0], expectedTypeName, paramValue);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void TestSqlDataReaderParameterToTVP_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataReaderParameterToTVP_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpVariant");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
                xsql(conn, string.Format("create type dbo.{0} as table (f1 sql_variant)", tvpTypeName));

                // Send TVP using SqlDataReader.
                using (SqlConnection connInput = new(s_connStr))
                {
                    connInput.Open();
                    using (SqlCommand cmdInput = connInput.CreateCommand())
                    {
                        cmdInput.CommandText = "select @p1 as f1";
                        cmdInput.Parameters.Add("@p1", SqlDbType2.Variant);
                        cmdInput.Parameters["@p1"].Value = paramValue;
                        using SqlDataReader drInput = cmdInput.ExecuteReader(CommandBehavior.CloseConnection);
                        using SqlCommand cmd = conn.CreateCommand();
                        cmd.CommandText = "select f1, sql_variant_property(f1,'BaseType') as BaseType from @tvpParam";
                        SqlParameter p = cmd.Parameters.AddWithValue("@tvpParam", drInput);
                        p.SqlDbType = SqlDbType2.Structured;
                        p.TypeName = string.Format("dbo.{0}", tvpTypeName);
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            dr.Read();
                            VerifyReaderTypeAndValue("Test SqlDataReader Parameter To TVP [Variant Type]", "SqlDbType2.Variant", dr, expectedTypeName, expectedBaseTypeName, paramValue);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropType(conn, tvpTypeName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void TestSqlDataReader_TVP_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataReader_TVP_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpType");
            string InputTableName = DataTestUtility.GetUniqueNameForSqlServer("InputTable");
            string OutputTableName = DataTestUtility.GetUniqueNameForSqlServer("OutputTable");
            string ProcName = DataTestUtility.GetUniqueNameForSqlServer("spTVPProc");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();

                DropStoredProcedure(conn, ProcName);
                DropTable(conn, InputTableName);
                DropTable(conn, OutputTableName);
                DropType(conn, $"dbo.{tvpTypeName}");

                xsql(conn, string.Format("create type dbo.{0} as table (f1 {1})", tvpTypeName, expectedBaseTypeName));
                xsql(conn, string.Format("create table {0} (f1 {1})", InputTableName, expectedBaseTypeName));
                xsql(conn, string.Format("create table {0} (f1 {1})", OutputTableName, expectedBaseTypeName));

                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("insert into {0} values(CAST('{1}' AS {2}))", InputTableName, value, expectedBaseTypeName));
                xsql(conn, string.Format("create proc {0} (@P {1} READONLY) as begin insert into {2} select * from @P; end", ProcName, tvpTypeName, OutputTableName));

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("SELECT * FROM {0}", InputTableName);
                using SqlDataReader r = cmd.ExecuteReader();
                using (SqlConnection conn2 = new(s_connStr))
                {
                    conn2.Open();
                    SqlCommand cmd2 = new(ProcName, conn2)
                    {
                        CommandType = CommandType.StoredProcedure
                    };
                    SqlParameter p = cmd2.Parameters.AddWithValue("@P", r);
                    p.SqlDbType = SqlDbType2.Structured;
                    p.TypeName = tvpTypeName;
                    cmd2.ExecuteNonQuery();

                    cmd2.CommandText = string.Format("SELECT f1 FROM {0}", OutputTableName);
                    cmd2.CommandType = CommandType.Text;
                    using (SqlDataReader dr = cmd2.ExecuteReader())
                    {
                        dr.Read();
                        VerifyReaderTypeAndValue("Test SqlDataReader TVP [Data Type]", expectedBaseTypeName, expectedTypeName, dr[0], expectedTypeName, paramValue);
                    }
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, ProcName);
                DropTable(conn, InputTableName);
                DropTable(conn, OutputTableName);
                DropType(conn, tvpTypeName);
            }
        }

        private static void TestSqlDataReader_TVP_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSqlDataReader_TVP_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string tvpTypeName = DataTestUtility.GetUniqueNameForSqlServer("tvpVariant_DRdrTVPVar");
            string InputTableName = DataTestUtility.GetUniqueNameForSqlServer("InputTable");
            string OutputTableName = DataTestUtility.GetUniqueNameForSqlServer("OutputTable");
            string ProcName = DataTestUtility.GetUniqueNameForSqlServer("spTVPProc_DRdrTVPVar");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();

                DropStoredProcedure(conn, ProcName);
                DropTable(conn, InputTableName);
                DropTable(conn, OutputTableName);
                DropType(conn, tvpTypeName);

                xsql(conn, string.Format("create type {0} as table (f1 sql_variant)", tvpTypeName));
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", InputTableName));
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", OutputTableName));

                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("insert into {0} values(CAST('{1}' AS {2}))", InputTableName, value, expectedBaseTypeName));
                xsql(conn, string.Format("create proc {0} (@P {1} READONLY) as begin insert into {2} select * from @P; end", ProcName, tvpTypeName, OutputTableName));

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("SELECT * FROM {0}", InputTableName);
                using SqlDataReader r = cmd.ExecuteReader();
                using (SqlConnection conn2 = new(s_connStr))
                {
                    conn2.Open();
                    using (SqlCommand cmd2 = new(ProcName, conn2))
                    {
                        cmd2.CommandType = CommandType.StoredProcedure;
                        SqlParameter p = cmd2.Parameters.AddWithValue("@P", r);
                        p.SqlDbType = SqlDbType2.Structured;
                        p.TypeName = tvpTypeName;
                        cmd2.ExecuteNonQuery();

                        cmd2.CommandText = string.Format("SELECT f1, sql_variant_property(f1,'BaseType') as BaseType FROM {0}", OutputTableName);
                        ;
                        cmd2.CommandType = CommandType.Text;
                        using (SqlDataReader dr = cmd2.ExecuteReader())
                        {
                            dr.Read();
                            VerifyReaderTypeAndValue("Test SqlDataReader TVP [Variant Type]", "SqlDbType2.Variant", dr, expectedTypeName, expectedBaseTypeName, paramValue);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, ProcName);
                DropTable(conn, InputTableName);
                DropTable(conn, OutputTableName);
                DropType(conn, tvpTypeName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void TestSimpleDataReader_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSimpleDataReader_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string inputTable = DataTestUtility.GetUniqueNameForSqlServer("inputTable");
            string procName = DataTestUtility.GetUniqueNameForSqlServer("paramProc3");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, inputTable);
                DropStoredProcedure(conn, procName);

                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("create table {0} (f1 {1})", inputTable, expectedBaseTypeName));
                xsql(conn, string.Format("insert into {0}(f1) values('{1}');", inputTable, value));
                xsql(conn, string.Format("create proc {0} as begin select f1 from {1} end;", procName, inputTable));

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test Simple Data Reader [Data Type]", expectedBaseTypeName, expectedTypeName, dr[0], expectedTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
                DropTable(conn, inputTable);
            }
        }

        private static void TestSimpleDataReader_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "TestSimpleDataReader_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string inputTable = DataTestUtility.GetUniqueNameForSqlServer("inputTable");
            string procName = DataTestUtility.GetUniqueNameForSqlServer("paramProc4");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, inputTable);
                DropStoredProcedure(conn, procName);

                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", inputTable));
                xsql(conn, string.Format("insert into {0}(f1) values(CAST('{1}' AS {2}));", inputTable, value, expectedBaseTypeName));
                xsql(conn, string.Format("create proc {0} as begin select f1, sql_variant_property(f1,'BaseType') as BaseType from {1} end;", procName, inputTable));

                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = procName;
                cmd.CommandType = CommandType.StoredProcedure;
                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    VerifyReaderTypeAndValue("Test Simple Data Reader [Variant Type]", "SqlDbType2.Variant", dr, expectedTypeName, expectedBaseTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropStoredProcedure(conn, procName);
                DropTable(conn, inputTable);
            }
        }

        private static void SqlBulkCopySqlDataReader_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopySqlDataReader_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopySrcTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkSrcTable");
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestTable");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 {1})", bulkCopyTableName, expectedBaseTypeName));

                DropTable(conn, bulkCopySrcTableName);
                xsql(conn, string.Format("create table {0} (f1 {1})", bulkCopySrcTableName, expectedBaseTypeName));
                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("insert into {0}(f1) values(CAST('{1}' AS {2}));", bulkCopySrcTableName, value, expectedBaseTypeName));

                using SqlConnection connInput = new(s_connStr);
                connInput.Open();
                using (SqlCommand cmdInput = connInput.CreateCommand())
                {
                    cmdInput.CommandText = string.Format("select * from {0}", bulkCopySrcTableName);
                    using SqlDataReader drInput = cmdInput.ExecuteReader();
                    // Perform bulk copy to target.
                    using (SqlBulkCopy bulkCopy = new(conn))
                    {
                        bulkCopy.BulkCopyTimeout = 60;
                        bulkCopy.BatchSize = 1;
                        bulkCopy.DestinationTableName = bulkCopyTableName;
                        bulkCopy.WriteToServer(drInput);
                    }

                    // Verify target.
                    using SqlCommand cmd = conn.CreateCommand();
                    cmd.CommandText = string.Format("select f1 from {0}", bulkCopyTableName);
                    using (SqlDataReader drVerify = cmd.ExecuteReader())
                    {
                        drVerify.Read();
                        VerifyReaderTypeAndValue("SqlBulkCopy From SqlDataReader [Data Type]", expectedBaseTypeName, expectedTypeName, drVerify[0], expectedTypeName, paramValue);
                    }
                }
                connInput.Close();
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                DropTable(conn, bulkCopySrcTableName);
            }

        }

        private static void SqlBulkCopySqlDataReader_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopySqlDataReader_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopySrcTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkSrcTable");
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestTable");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", bulkCopyTableName));

                DropTable(conn, bulkCopySrcTableName);
                xsql(conn, string.Format("create table {0} (f1 {1})", bulkCopySrcTableName, expectedBaseTypeName));
                string value = string.Empty;
                if (paramValue.GetType() == typeof(DateTimeOffset))
                {
                    DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                    value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
                }
                else if (paramValue.GetType() == typeof(TimeSpan))
                {
                    value = ((TimeSpan)paramValue).ToString();
                }
                else
                {
                    value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
                }
                xsql(conn, string.Format("insert into {0}(f1) values(CAST('{1}' AS {2}));", bulkCopySrcTableName, value, expectedBaseTypeName));

                using (SqlConnection connInput = new(s_connStr))
                {
                    connInput.Open();
                    using (SqlCommand cmdInput = connInput.CreateCommand())
                    {
                        cmdInput.CommandText = string.Format("select * from {0}", bulkCopySrcTableName);
                        using SqlDataReader drInput = cmdInput.ExecuteReader();
                        {
                            // Perform bulk copy to target.
                            using (SqlBulkCopy bulkCopy = new(conn))
                            {
                                bulkCopy.BulkCopyTimeout = 60;
                                bulkCopy.BatchSize = 1;
                                bulkCopy.DestinationTableName = bulkCopyTableName;
                                bulkCopy.WriteToServer(drInput);
                            }

                            // Verify target.
                            using SqlCommand cmd = conn.CreateCommand();
                            cmd.CommandText = string.Format("select f1, sql_variant_property(f1,'BaseType') as BaseType from {0}", bulkCopyTableName);
                            using (SqlDataReader drVerify = cmd.ExecuteReader())
                            {
                                drVerify.Read();
                                VerifyReaderTypeAndValue("SqlBulkCopy From SqlDataReader [Variant Type]", "SqlDbType2.Variant", drVerify, expectedTypeName, expectedBaseTypeName, paramValue);
                            }
                        }
                    }
                }

                conn.Close();
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                DropTable(conn, bulkCopySrcTableName);
            }
        }

        private static void SqlBulkCopyDataTable_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopyDataTable_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestType");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 {1})", bulkCopyTableName, expectedBaseTypeName));

                // Send using DataTable as source.
                DataTable t = new();
                t.Columns.Add("f1", paramValue.GetType());
                t.Rows.Add(new object[] { paramValue });

                // Perform bulk copy to target.
                using (SqlBulkCopy bulkCopy = new(conn))
                {
                    bulkCopy.BulkCopyTimeout = 60;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.DestinationTableName = bulkCopyTableName;
                    bulkCopy.WriteToServer(t, DataRowState.Added);
                }

                // Verify target.
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select f1 from {0}", bulkCopyTableName);
                using (SqlDataReader drVerify = cmd.ExecuteReader())
                {
                    drVerify.Read();
                    VerifyReaderTypeAndValue("SqlBulkCopy From Data Table [Data Type]", expectedBaseTypeName, expectedTypeName, drVerify[0], expectedTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                {
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                }
                else if (IsExpectedInvalidOperationException(e, expectedBaseTypeName))
                {
                    LogMessage(tag, "[EXPECTED INVALID OPERATION EXCEPTION] " + AmendTheGivenMessageDateValueException(e.Message, paramValue));
                }
                else
                {
                    DisplayError(tag, e);
                }
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void SqlBulkCopyDataTable_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopyDataTable_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestVariant");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", bulkCopyTableName));

                // Send using DataTable as source.
                DataTable t = new();
                t.Columns.Add("f1", typeof(object));
                t.Rows.Add(new object[] { paramValue });

                // Perform bulk copy to target.
                using (SqlBulkCopy bulkCopy = new(conn))
                {
                    bulkCopy.BulkCopyTimeout = 60;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.DestinationTableName = bulkCopyTableName;
                    bulkCopy.WriteToServer(t, DataRowState.Added);
                }

                // Verify target.
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select f1, sql_variant_property(f1,'BaseType') as BaseType from {0}", bulkCopyTableName);
                using (SqlDataReader drVerify = cmd.ExecuteReader())
                {
                    drVerify.Read();
                    VerifyReaderTypeAndValue("SqlBulkCopy From Data Table [Variant Type]", "SqlDbType2.Variant", drVerify, expectedTypeName, expectedBaseTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
            }
        }

        private static void SqlBulkCopyDataRow_Type(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopyDataRow_Type";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestType");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 {1})", bulkCopyTableName, expectedBaseTypeName));
                DataTable t = new();
                t.Columns.Add("f1", paramValue.GetType());
                t.Rows.Add(new object[] { paramValue });
                DataRow[] rowToSend = t.Select();
                using (SqlBulkCopy bulkCopy = new(conn))
                {
                    bulkCopy.BulkCopyTimeout = 60;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.DestinationTableName = bulkCopyTableName;
                    bulkCopy.WriteToServer(rowToSend);
                }
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select f1 from {0}", bulkCopyTableName);
                using (SqlDataReader drVerify = cmd.ExecuteReader())
                {
                    drVerify.Read();
                    VerifyReaderTypeAndValue("SqlBulkCopy From Data Row [Data Type]", expectedBaseTypeName, expectedTypeName, drVerify[0], expectedTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                {
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                }
                else if (IsExpectedInvalidOperationException(e, expectedBaseTypeName))
                {
                    LogMessage(tag, "[EXPECTED INVALID OPERATION EXCEPTION] " + AmendTheGivenMessageDateValueException(e.Message, paramValue));
                }
                else
                {
                    DisplayError(tag, e);
                }
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
            }
        }

        // sql_variant parameters and sql_variant TVPs store all datetime values internally
        // as datetime, hence breaking for 2008 types
        private static void SqlBulkCopyDataRow_Variant(object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            string tag = "SqlBulkCopyDataRow_Variant";
            DisplayHeader(tag, paramValue, expectedBaseTypeName);
            string bulkCopyTableName = DataTestUtility.GetUniqueNameForSqlServer("bulkDestVariant");
            try
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
                xsql(conn, string.Format("create table {0} (f1 sql_variant)", bulkCopyTableName));
                DataTable t = new();
                t.Columns.Add("f1", typeof(object));
                t.Rows.Add(new object[] { paramValue });
                DataRow[] rowToSend = t.Select();
                using (SqlBulkCopy bulkCopy = new(conn))
                {
                    bulkCopy.BulkCopyTimeout = 60;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.DestinationTableName = bulkCopyTableName;
                    bulkCopy.WriteToServer(rowToSend);
                }
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select f1, sql_variant_property(f1,'BaseType') as BaseType from {0}", bulkCopyTableName);
                using (SqlDataReader drVerify = cmd.ExecuteReader())
                {
                    drVerify.Read();
                    VerifyReaderTypeAndValue("SqlBulkCopy From Data Row [Variant Type]", "SqlDbType2.Variant", drVerify, expectedTypeName, expectedBaseTypeName, paramValue);
                }
            }
            catch (Exception e)
            {
                if (IsExpectedException(e, paramValue, expectedTypeName, expectedBaseTypeName))
                    LogMessage(tag, "[EXPECTED EXCEPTION] " + e.Message);
                else
                    DisplayError(tag, e);
            }
            finally
            {
                using SqlConnection conn = new(s_connStr);
                conn.Open();
                DropTable(conn, bulkCopyTableName);
            }
        }

        // NOTE: Actions

        private static SqlDbType GetSqlDbType(string expectedBaseTypeName)
        {
            return expectedBaseTypeName.ToLowerInvariant() switch
            {
                "time" => SqlDbType2.Time,
                "date" => SqlDbType2.Date,
                "smalldatetime" => SqlDbType2.SmallDateTime,
                "datetime" => SqlDbType2.DateTime,
                "datetime2" => SqlDbType2.DateTime2,
                "datetimeoffset" => SqlDbType2.DateTimeOffset,
                _ => SqlDbType2.Variant,
            };
        }

        private static void xsql(SqlConnection conn, string sql)
        {
            using SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static void DropStoredProcedure(SqlConnection conn, string procName)
        {
            xsql(conn, string.Format("if exists(select 1 from sys.procedures where name='{0}') begin drop proc {1} end", procName.Substring(1, procName.Length - 2), procName));
        }

        private static void DropTable(SqlConnection conn, string tableName)
        {
            xsql(conn, string.Format("if exists(select 1 from sys.tables where name='{0}') begin drop table {1} end", tableName.Substring(1, tableName.Length - 2), tableName));
        }

        private static void DropType(SqlConnection conn, string typeName)
        {
            xsql(conn, string.Format("if exists(select 1 from sys.types where name='{0}') begin drop type {1} end", typeName.Substring(1, typeName.Length - 2), typeName));
        }

        // NOTE: Checking and Verification
        private static void VerifyReaderTypeAndValue(string tag, string expectedBaseTypeName, string type, object actualValue, string expectedTypeName, object expectedValue)
        {
            string actualTypeName = actualValue.GetType().ToString();

            LogValues(tag, expectedTypeName, string.Empty, expectedValue, actualTypeName, string.Empty, actualValue);

            if (!actualTypeName.Equals(expectedTypeName))
            {
                string ErrorMessage = string.Format(">>> ERROR: TYPE MISMATCH!!! [Actual = {0}] [Expected = {1}]",
                    actualTypeName,
                    expectedTypeName);

                LogMessage(tag, ErrorMessage);
            }
            if (!actualValue.Equals(expectedValue))
            {
                string ErrorMessage;
                if (IsValueCorrectForType(expectedBaseTypeName, expectedValue, actualValue))
                {
                    ErrorMessage = string.Format("[EXPECTED ERROR]: VALUE MISMATCH - [Actual = {0}] [Expected = {1}]",
                    DataTestUtility.GetValueString(actualValue),
                    DataTestUtility.GetValueString(expectedValue));
                }
                else
                {
                    ErrorMessage = string.Format(">>> ERROR: VALUE MISMATCH!!! [Actual = {0}] [Expected = {1}]",
                    DataTestUtility.GetValueString(actualValue),
                    DataTestUtility.GetValueString(expectedValue));
                }
                LogMessage(tag, ErrorMessage);
            }
        }

        private static void VerifyReaderTypeAndValue(string tag, string type, SqlDataReader dr, string expectedTypeName, string expectedBaseTypeName, object expectedValue)
        {
            object actualValue = dr[0];
            string actualTypeName = actualValue.GetType().ToString();
            string actualBaseTypeName = dr.GetString(1);

            LogValues(tag, expectedTypeName, expectedBaseTypeName, expectedValue, actualTypeName, actualBaseTypeName, actualValue);

            if (!actualTypeName.Equals(expectedTypeName))
            {
                string ErrorMessage = string.Format(">>> ERROR: TYPE MISMATCH!!! [Actual = {0}] [Expected = {1}]",
                    actualTypeName,
                    expectedTypeName);
                LogMessage(tag, ErrorMessage);
            }
            if (!actualBaseTypeName.Equals(expectedBaseTypeName))
            {
                if (((actualTypeName.ToLowerInvariant() != "system.datetime") || (actualTypeName.ToLowerInvariant() != "system.datetimeoffset"))
                    && (actualBaseTypeName != "datetime2"))
                {
                    string ErrorMessage = string.Format(">>> ERROR: VARIANT BASE TYPE MISMATCH!!! [Actual = {0}] [Expected = {1}]",
                        actualBaseTypeName,
                        expectedBaseTypeName);
                    LogMessage(tag, ErrorMessage);
                }
            }
            if (!actualValue.Equals(expectedValue))
            {
                string ErrorMessage;
                if (IsValueCorrectForType(expectedBaseTypeName, expectedValue, actualValue))
                {
                    ErrorMessage = string.Format("[EXPECTED ERROR]: VALUE MISMATCH - [Actual = {0}] [Expected = {1}]",
                    DataTestUtility.GetValueString(actualValue),
                    DataTestUtility.GetValueString(expectedValue));
                }
                else
                {
                    ErrorMessage = string.Format(">>> ERROR: VALUE MISMATCH!!! [Actual = {0}] [Expected = {1}]",
                    DataTestUtility.GetValueString(actualValue),
                    DataTestUtility.GetValueString(expectedValue));
                }
                LogMessage(tag, ErrorMessage);
            }
        }

        private static bool IsValueCorrectForType(string expectedBaseTypeName, object expectedValue, object actualValue)
        {
            switch (expectedBaseTypeName)
            {
                case "date":
                    if (((DateTime)expectedValue).ToString("M/d/yyyy").Equals(((DateTime)actualValue).ToString("M/d/yyyy")))
                        return true;
                    else
                        return false;
                case "datetime":
                    if ((((DateTime)expectedValue).Ticks == 3155378975999999999) &&
                        (((DateTime)actualValue).Ticks == 3155378975999970000))
                        return true;
                    else
                        return false;
                default:
                    return false;
            }
        }

        private static bool IsExpectedException(Exception e, object paramValue, string expectedTypeName, string expectedBaseTypeName)
        {
            if ((e.GetType() == typeof(System.Data.SqlTypes.SqlTypeException)) &&
                                (expectedBaseTypeName == "datetime") &&
                                (e.Message.Contains("1753")) &&
                                (((DateTime)paramValue).Year < 1753))
            {
                return true;
            }
            else if ((e.GetType() == typeof(SqlException)) &&
                                (expectedBaseTypeName == "datetime") &&
                                (e.Message.Contains("conversion of a varchar data type to a datetime data type")) &&
                                (((DateTime)paramValue).Year < 1753))
            {
                return true;
            }
            else if ((e.GetType() == typeof(SqlException)) &&
                                (expectedBaseTypeName == "datetime") &&
                                (e.Message.Contains("converting date and/or time from character string")) &&
                                (((DateTime)paramValue) == DateTime.MaxValue))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private static bool IsExpectedInvalidOperationException(Exception e, string expectedBaseTypeName)
        {
            return ((e.GetType() == typeof(InvalidOperationException)) &&
                    (expectedBaseTypeName == "time") &&
                    (e.Message.Contains("The given value ")));
        }

        private static string AmendTheGivenMessageDateValueException(string message, object paramValue)
        {
            string value;
            if (paramValue.GetType() == typeof(DateTimeOffset))
            {
                DateTime dt = ((DateTimeOffset)paramValue).UtcDateTime;
                value = dt.ToString("M/d/yyyy") + " " + dt.TimeOfDay;
            }
            else if (paramValue.GetType() == typeof(TimeSpan))
            {
                value = ((TimeSpan)paramValue).ToString();
            }
            else
            {
                value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
            }

            return message.Replace(paramValue.ToString(), value);
        }

        // NOTE: Logging and Display
        private static void DisplayHeader(string tag, object paramValue, string expectedBaseTypeName)
        {
            Console.WriteLine("");
            string value;
            if (paramValue.GetType() == typeof(DateTimeOffset))
            {
                DateTimeOffset dt = (DateTimeOffset)paramValue;
                value = dt.DateTime.ToString("M/d/yyyy") + " " + dt.DateTime.TimeOfDay + " " + dt.Offset;
            }
            else if (paramValue.GetType() == typeof(TimeSpan))
            {
                value = ((TimeSpan)paramValue).ToString();
            }
            else
            {
                value = ((DateTime)paramValue).ToString("M/d/yyyy") + " " + ((DateTime)paramValue).TimeOfDay;
            }

            Console.WriteLine(string.Format("------------------------------ {0} [type: {1} value:{2}] ------------------------------", tag, expectedBaseTypeName, value));
        }

        private static void DisplayError(string tag, Exception e)
        {
            string ExceptionMessage = string.Format(">>> EXCEPTION: [{0}] {1}", e.GetType(), e.Message);

            // InvalidCastException message is different between core and framework for this cast attempt.
            // Make them the same for the purposes of comparing test output.
            if (e is InvalidCastException &&
                e.Message.Equals("Unable to cast object of type 'System.TimeSpan' to type 'System.DateTime'."))
                ExceptionMessage = string.Format(">>> EXCEPTION: [{0}] {1}", e.GetType(), "Specified cast is not valid.");

            if (e is ArgumentOutOfRangeException &&
                e.Message.Equals("The added or subtracted value results in an un-representable DateTime. (Parameter 'value')"))
            {
                ExceptionMessage = string.Format(">>> EXCEPTION: [{0}] {1}", e.GetType(), "The added or subtracted value results in an un-representable DateTime.\nParameter name: value");
            }

            LogMessage(tag, ExceptionMessage);
        }

        private static void LogMessage(string tag, string message)
        {
            Console.WriteLine(string.Format("{0}{1}", tag, message));
        }

        private static void LogValues(string tag, string expectedTypeName, string expectedBaseTypeName, object expectedValue, string actualTypeName, string actualBaseTypeName, object actualValue)
        {
            Console.WriteLine(string.Format("Type        => Expected : Actual == {0} : {1}", expectedTypeName, actualTypeName));
            Console.WriteLine(string.Format("Base Type   => Expected : Actual == {0} : {1}", expectedBaseTypeName, actualBaseTypeName));
            if (expectedTypeName == "System.DateTimeOffset")
                Console.WriteLine(string.Format("Value       => Expected : Actual == {0} : {1}", ((DateTimeOffset)expectedValue).Ticks.ToString(), ((DateTimeOffset)actualValue).Ticks.ToString()));
            else if (expectedTypeName == "System.DateTime")
                Console.WriteLine(string.Format("Value       => Expected : Actual == {0} : {1}", ((DateTime)expectedValue).Ticks.ToString(), ((DateTime)actualValue).Ticks.ToString()));
        }
    }
}
