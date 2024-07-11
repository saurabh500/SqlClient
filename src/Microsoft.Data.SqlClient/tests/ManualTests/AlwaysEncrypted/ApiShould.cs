// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider;
using Microsoft.Data.SqlClient.ManualTesting.Tests.AlwaysEncrypted.Setup;
using Microsoft.Data.SqlClient.ManualTesting.Tests.SystemDataInternals;
using Xunit;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests.AlwaysEncrypted
{
    /// <summary>
    /// Always Encrypted public API Manual tests.
    /// </summary>
    public sealed class ApiShould : IClassFixture<PlatformSpecificTestContext>, IDisposable
    {
        private SQLSetupStrategy _fixture;

        private readonly string _tableName;

        private Dictionary<string, SqlColumnEncryptionKeyStoreProvider> _requiredProvider = new()
        {
            { DummyKeyStoreProvider.Name, new DummyKeyStoreProvider() }
        };

        private const string NotRequiredProviderName = "DummyProvider2";
        private Dictionary<string, SqlColumnEncryptionKeyStoreProvider> _notRequiredProvider = new()
        {
            { NotRequiredProviderName, new DummyKeyStoreProvider() }
        };

        private string _failedToDecryptMessage;
        private string _providerNotFoundMessage = string.Format(
            SystemDataResourceManager.Instance.TCE_UnrecognizedKeyStoreProviderName,
            DummyKeyStoreProvider.Name,
            "'MSSQL_CERTIFICATE_STORE', 'MSSQL_CNG_STORE', 'MSSQL_CSP_PROVIDER'",
            $"'{NotRequiredProviderName}'");

        public ApiShould(PlatformSpecificTestContext context)
        {
            _fixture = context.Fixture;
            _tableName = _fixture.ApiTestTable.Name;

            ApiTestTable _customKeyStoreProviderTable = _fixture.CustomKeyStoreProviderTestTable as ApiTestTable;
            byte[] encryptedCek = _customKeyStoreProviderTable.columnEncryptionKey1.EncryptedValue;
            string _lastTenBytesCek = BitConverter.ToString(encryptedCek, encryptedCek.Length - 10, 10);
            _failedToDecryptMessage = string.Format(SystemDataResourceManager.Instance.TCE_KeyDecryptionFailed,
                DummyKeyStoreProvider.Name, _lastTenBytesCek);
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithBooleanVariable))]
        public void TestSqlTransactionCommitRollbackWithTransparentInsert(string connection, bool isCommitted)
        {
            CleanUpTable(connection, _tableName);

            using SqlConnection sqlConnection = new(connection);
            sqlConnection.Open();

            Customer customer = new Customer(40, "Microsoft", "Corporation");

            // Start a transaction and either commit or rollback based on the test variation.
            using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction())
            {
                DatabaseHelper.InsertCustomerData(sqlConnection, sqlTransaction, _tableName, customer);

                if (isCommitted)
                {
                    sqlTransaction.Commit();
                }
                else
                {
                    sqlTransaction.Rollback();
                }
            }

            // Data should be available on select if committed else, data should not be available.
            if (isCommitted)
            {
                DatabaseHelper.VerifyRecordPresent(sqlConnection, customer, _tableName);
            }
            else
            {
                DatabaseHelper.VerifyRecordAbsent(sqlConnection, customer, _tableName);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestSqlTransactionRollbackToSavePoint(string connection)
        {
            CleanUpTable(connection, _tableName);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                // Start a new transaction, with isolation level as read uncommitted, so we will be able to read the inserted records without committing.
                using (SqlTransaction sqlTransaction = sqlConnection.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                {
                    // Insert row no:1 and Save the state of the transaction to a named check point.
                    Customer customer1 = new(50, "Microsoft2", "Corporation2");
                    DatabaseHelper.InsertCustomerData(sqlConnection, sqlTransaction, _tableName, customer1);
                    sqlTransaction.Save(@"checkpoint");

                    // Insert row no:2
                    Customer customer2 = new(60, "Microsoft3", "Corporation3");
                    DatabaseHelper.InsertCustomerData(sqlConnection, sqlTransaction, _tableName, customer2);

                    // Read the data that was just inserted, both Row no:2 and Row no:1 should be available.
                    DatabaseHelper.VerifyRecordPresent(sqlConnection, customer1, _tableName, sqlTransaction);

                    // Try to read the just inserted record under read-uncommitted mode.
                    DatabaseHelper.VerifyRecordPresent(sqlConnection, customer2, _tableName, sqlTransaction);

                    // Rollback the transaction to the saved checkpoint, to lose the row no:2.
                    sqlTransaction.Rollback(@"checkpoint");

                    // Row no:2 should not be available.
                    DatabaseHelper.VerifyRecordAbsent(sqlConnection, customer2, _tableName, sqlTransaction);

                    // Row no:1 should still be available.
                    DatabaseHelper.VerifyRecordPresent(sqlConnection, customer1, _tableName, sqlTransaction);

                    // Completely rollback the transaction.
                    sqlTransaction.Rollback();

                    // Now even row no:1 should not be available.
                    DatabaseHelper.VerifyRecordAbsent(sqlConnection, customer1, _tableName, sqlTransaction);
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void SqlParameterProperties(string connection)
        {
            string tableName = _fixture.SqlParameterPropertiesTable.Name;
            const string firstColumnName = @"firstColumn";
            const string secondColumnName = @"secondColumn";
            const string thirdColumnName = @"thirdColumn";
            string inputProcedureName = DataTestUtility.GetUniqueName("InputProc").ToString();
            string outputProcedureName = DataTestUtility.GetUniqueName("OutputProc").ToString();
            const int charColumnSize = 100;
            const int decimalColumnPrecision = 10;
            const int decimalColumnScale = 4;
            const int timeColumnScale = 5;

            CleanUpTable(connection, tableName);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                try
                {
                    sqlConnection.Open();

                    // Create a procedure that gets input parameters that have smaller data types than the actual columns types.
                    // Decimal precision and scale need to match exactly.
                    int charInputParamSize = charColumnSize - 20;
                    int decimalInputParamPrecision = decimalColumnPrecision;
                    int decimalInputParamScale = decimalColumnScale;
                    int timeInputParamScale = timeColumnScale - 1;

                    using (SqlCommand sqlCmd = new SqlCommand(string.Format(
                        @"CREATE PROCEDURE {0} (@p1 nvarchar({5}), @p2 decimal ({6}, {7}), @p3 time ({8})) AS
                            SELECT * FROM [{1}] WHERE {2} = @p1 AND {3} = @p2 AND {4} = @p3",
                        inputProcedureName, tableName, firstColumnName, secondColumnName, thirdColumnName, charInputParamSize, decimalInputParamPrecision, decimalInputParamScale, timeInputParamScale), sqlConnection))
                    {
                        sqlCmd.ExecuteNonQuery();
                    }

                    // Create a procedure that returns output parameters that have larger data type than the actual column types.
                    // Decimal precision and scale need to match exactly.
                    int charOutputParamSize = charColumnSize + 20;
                    int decimalOutputParamPrecision = decimalColumnPrecision;
                    int decimalOutputParamScale = decimalColumnScale;
                    int timeOutputParamScale = timeColumnScale + 1;

                    using (SqlCommand sqlCmd = new SqlCommand(string.Format(
                        @"CREATE PROCEDURE {0} (@p1 nvarchar({5}) OUTPUT, @p2 decimal ({6}, {7}) OUTPUT, @p3 time ({8}) OUTPUT) AS
                            SELECT @p1={2}, @p2={3}, @p3={4} FROM [{1}]",
                        outputProcedureName, tableName, firstColumnName, secondColumnName, thirdColumnName, charOutputParamSize, decimalOutputParamPrecision, decimalOutputParamScale, timeOutputParamScale), sqlConnection))
                    {
                        sqlCmd.ExecuteNonQuery();
                    }

                    // Insert a row.
                    using (SqlCommand sqlCmd = new SqlCommand(
                        cmdText: $"INSERT INTO [{tableName}] VALUES (@p1, @p2, @p3)",
                        connection: sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        SqlParameter param1 = new SqlParameter("@p1", SqlDbType2.NVarChar)
                        {
                            Size = charColumnSize,
                            Value = "ColumnValue"
                        };
                        sqlCmd.Parameters.Add(param1);

                        SqlParameter param2 = new SqlParameter("@p2", SqlDbType2.Decimal)
                        {
                            Precision = decimalColumnPrecision,
                            Scale = decimalColumnScale,
                            Value = 400.21
                        };
                        sqlCmd.Parameters.Add(param2);

                        SqlParameter param3 = new SqlParameter("@p3", SqlDbType2.Time)
                        {
                            Scale = timeColumnScale,
                            Value = TimeSpan.Parse("1:01:01.001")
                        };
                        sqlCmd.Parameters.Add(param3);

                        sqlCmd.ExecuteNonQuery();
                    }

                    // Now execute the procedure with input params and make sure the parameter properties stays as set.
                    using (SqlCommand sqlCmd = new SqlCommand(inputProcedureName, sqlConnection, null, SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        sqlCmd.CommandType = CommandType.StoredProcedure;

                        // Set the actual parameter size to even smaller than the proc size. This is allowed since we assign from these values into the params.
                        // Decimal precision and scale need to match exactly.
                        int charParamSize = charInputParamSize - 20;
                        int decimalParamPrecision = decimalInputParamPrecision;
                        int decimalParamScale = decimalInputParamScale;
                        int timeParamScale = timeInputParamScale - 1;

                        SqlParameter param1 = new SqlParameter("@p1", SqlDbType2.NVarChar)
                        {
                            Size = charParamSize,
                            Value = "ColumnValue"
                        };
                        sqlCmd.Parameters.Add(param1);

                        SqlParameter param2 = new SqlParameter("@p2", SqlDbType2.Decimal)
                        {
                            Precision = (byte)decimalParamPrecision,
                            Scale = (byte)decimalParamScale,
                            Value = 400.21
                        };
                        sqlCmd.Parameters.Add(param2);

                        SqlParameter param3 = new SqlParameter("@p3", SqlDbType2.Time)
                        {
                            Scale = (byte)timeParamScale,
                            Value = TimeSpan.Parse("1:01:01.001")
                        };
                        sqlCmd.Parameters.Add(param3);

                        using (SqlDataReader reader = sqlCmd.ExecuteReader())
                        {
                            Assert.True(reader.Read(), "We should have found one row.");
                            Assert.False(reader.Read(), "We shouldn't have found a second row.");
                        }

                        // Validate that all properties have stayed the same for all parameters.
                        Assert.Equal(SqlDbType2.NVarChar, param1.SqlDbType);
                        Assert.Equal(DbType.String, param1.DbType);
                        Assert.Equal(0, param1.Scale);
                        Assert.Equal(0, param1.Precision);
                        Assert.Equal(charParamSize, param1.Size);

                        Assert.Equal(SqlDbType2.Decimal, param2.SqlDbType);
                        Assert.Equal(DbType.Decimal, param2.DbType);
                        Assert.Equal(decimalParamScale, param2.Scale);
                        Assert.Equal(decimalParamPrecision, param2.Precision);
                        Assert.Equal(0, param2.Size);

                        Assert.Equal(SqlDbType2.Time, param3.SqlDbType);
                        Assert.Equal(DbType.Time, param3.DbType);
                        Assert.Equal(timeParamScale, param3.Scale);
                        Assert.Equal(0, param3.Precision);
                        Assert.Equal(0, param3.Size);
                    }

                    // Now execute the procedure with output params and make sure the parameter properties stays as set.
                    using (SqlCommand sqlCmd = new SqlCommand(outputProcedureName, sqlConnection, null, SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        sqlCmd.CommandType = CommandType.StoredProcedure;

                        // For output params the type needs to be identical with the actual procedure parameter since we will assign in both directions.
                        int charParamSize = charOutputParamSize;
                        int decimalParamPrecision = decimalOutputParamPrecision;
                        int decimalParamScale = decimalOutputParamScale;
                        int timeParamScale = timeOutputParamScale;

                        SqlParameter param1 = new SqlParameter("@p1", SqlDbType2.NVarChar)
                        {
                            Direction = ParameterDirection.Output,
                            Size = charParamSize,
                            Value = "DifferentColumnValue"
                        };

                        sqlCmd.Parameters.Add(param1);

                        SqlParameter param2 = new SqlParameter("@p2", SqlDbType2.Decimal)
                        {
                            Direction = ParameterDirection.Output,
                            Precision = (byte)decimalParamPrecision,
                            Scale = (byte)decimalParamScale,
                            Value = 4000.21
                        };
                        sqlCmd.Parameters.Add(param2);

                        SqlParameter param3 = new SqlParameter("@p3", SqlDbType2.Time)
                        {
                            Direction = ParameterDirection.Output,
                            Scale = (byte)timeParamScale,
                            Value = TimeSpan.Parse("1:01:01.01")
                        };
                        sqlCmd.Parameters.Add(param3);
                        sqlCmd.ExecuteNonQuery();

                        // Validate that all properties have stayed the same for all parameters.
                        Assert.Equal(SqlDbType2.NVarChar, param1.SqlDbType);
                        Assert.Equal(DbType.String, param1.DbType);
                        Assert.Equal(0, param1.Scale);
                        Assert.Equal(0, param1.Precision);
                        Assert.Equal(charParamSize, param1.Size);

                        Assert.Equal(SqlDbType2.Decimal, param2.SqlDbType);
                        Assert.Equal(DbType.Decimal, param2.DbType);
                        Assert.Equal(decimalParamScale, param2.Scale);
                        Assert.Equal(decimalParamPrecision, param2.Precision);
                        Assert.Equal(0, param2.Size);

                        Assert.Equal(SqlDbType2.Time, param3.SqlDbType);
                        Assert.Equal(DbType.Time, param3.DbType);
                        Assert.Equal(timeParamScale, param3.Scale);
                        Assert.Equal(0, param3.Precision);
                        Assert.Equal(0, param3.Size);
                    }
                }
                finally
                {
                    DropHelperProcedures(new string[] { inputProcedureName, outputProcedureName }, connection);
                }

            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestSqlDataAdapterFillDataTable(string connection)
        {
            CleanUpTable(connection, _tableName);

            const string DummyParamName = "@dummyParam";
            int numberOfRows = 100;

            IList<object> values = GetValues(dataHint: 71);

            InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            var encryptionEnabledConnectionString = new SqlConnectionStringBuilder(connection)
            {
                ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled
            }.ConnectionString;

            using (var sqlConnection = new SqlConnection(encryptionEnabledConnectionString))
            {
                sqlConnection.Open();

                // Create a command with an encrypted parameter to confirm that parameters work ocrrectly for Fill.
                using (SqlCommand cmd = new SqlCommand(
                    cmdText: $"select * from [{_tableName}] where FirstName != {DummyParamName} and CustomerId = @CustomerId",
                    connection: sqlConnection))
                {
                    if (DataTestUtility.EnclaveEnabled)
                    {
                        //Increase Time out for enclave-enabled server.
                        cmd.CommandTimeout = 90;
                    }

                    SqlParameter dummyParam = new SqlParameter(DummyParamName, SqlDbType2.NVarChar, 150)
                    {
                        Value = "a"
                    };

                    cmd.Parameters.Add(dummyParam);
                    cmd.Parameters.AddWithValue(@"CustomerId", values[0]);

                    // Fill the data table from the results of select statement.
                    using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(cmd))
                    {
                        DataTable dataTable = new DataTable();
                        sqlDataAdapter.Fill(dataTable);

                        TestDataAdapterFillResults(dataTable, values);

                        // Try refilling another table with the same adapter to make sure that reusing the command works correctly.
                        dataTable = new DataTable();
                        sqlDataAdapter.Fill(dataTable);
                        TestDataAdapterFillResults(dataTable, values);
                        Assert.Equal(numberOfRows, dataTable.Rows.Count);

                        // Use the Fill overload which fills in a dataset.
                        DataSet dataSet = new DataSet();
                        sqlDataAdapter.Fill(dataSet, _tableName);
                        Assert.Single(dataSet.Tables);
                        Assert.Equal(numberOfRows, dataSet.Tables[0].Rows.Count);
                        TestDataAdapterFillResults(dataSet.Tables[0], values);

                        // Use the Fill overload which lets you specify the max number of records to be fetched.
                        dataSet = new DataSet();
                        sqlDataAdapter.Fill(dataSet, 0, 1, _tableName);
                        Assert.Single(dataSet.Tables);
                        Assert.Single(dataSet.Tables[0].Rows);
                        TestDataAdapterFillResults(dataSet.Tables[0], values);
                    }
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithSchemaType))]
        public void TestSqlDataAdapterFillSchema(string connection, SchemaType schemaType)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 44);
            int numberOfRows = 42;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                SqlDataAdapter adapter = CreateSqlDataAdapter(sqlConnection);
                DataTable dataTable = new DataTable();
                DataTable dataTable2 = adapter.FillSchema(dataTable, schemaType);
                DataColumnCollection dataColumns = dataTable2.Columns;

                ValidateSchema(dataTable2.Columns);
                ValidateSchema(dataTable.Columns);

                // Test the FillSchema overload that takes in a dataset with SchemaType = Source.
                DataSet dataSet = new DataSet();
                DataTable[] dataSet2 = adapter.FillSchema(dataSet, schemaType);
                Assert.Single(dataSet2);

                ValidateSchema(dataSet2[0].Columns);
                ValidateSchema(dataSet.Tables[0].Columns);
            }
        }

        /// <summary>
        /// Validate the schema obtained SqlDataAdapter.FillSchema
        /// </summary>
        /// <param name="dataColumns"></param>
        private void ValidateSchema(DataColumnCollection dataColumns)
        {
            Assert.Equal(@"CustomerId", dataColumns[0].ColumnName);
            Assert.Equal(@"FirstName", dataColumns[1].ColumnName);
            Assert.Equal(@"LastName", dataColumns[2].ColumnName);
            Assert.Equal(typeof(int), dataColumns[0].DataType);
            Assert.Equal(typeof(string), dataColumns[1].DataType);
            Assert.Equal(typeof(string), dataColumns[2].DataType);
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithBooleanVariable))]
        public void TestExecuteNonQuery(string connection, bool isAsync)
        {
            CleanUpTable(connection, _tableName);

            Parallel.For(0, 10, i =>
            {
                bool tryagain = false;
                do
                {
                    IList<object> values = GetValues(dataHint: 45 + i + 1);
                    int numberOfRows = 10 + i;

                    // Insert a bunch of rows in to the table.
                    int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

                    Assert.Equal(numberOfRows, rowsAffected);
                    rowsAffected = -1;
                    using (SqlConnection sqlConnection = new SqlConnection(connection))
                    {
                        try
                        {
                            sqlConnection.Open();

                            // Update the set of rows that were inserted just now. And verify the rows affected as returned by ExecuteNonQuery.
                            using (SqlCommand sqlCommand = new SqlCommand(
                                cmdText: $"UPDATE [{_tableName}] SET FirstName = @FirstName WHERE CustomerId = @CustomerId",
                                connection: sqlConnection,
                                transaction: null,
                                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                            {
                                if (DataTestUtility.EnclaveEnabled)
                                {
                                    //Increase Time out for enclave-enabled server.
                                    sqlCommand.CommandTimeout = 90;
                                }

                                sqlCommand.Parameters.AddWithValue(@"FirstName", string.Format(@"Microsoft{0}", i + 100));
                                sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);

                                if (isAsync)
                                {
                                    Task<int> executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                                    rowsAffected = executeTask.Result;
                                }
                                else
                                {
                                    rowsAffected = sqlCommand.ExecuteNonQuery();
                                }
                            }
                        }
                        catch (SqlException sqle)
                        {
                            // SQL Server deadlocks are possible if test executes multiple parallel threads. We will try again.
                            if (sqle.ErrorCode.Equals(1205))
                            {
                                tryagain = true;
                                break;
                            }
                        }
                        Assert.Equal(numberOfRows, rowsAffected);
                        tryagain = false;
                    }
                } while (tryagain);
            });
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithBooleanVariable))]
        public void TestExecuteScalar(string connection, bool isAsync)
        {
            CleanUpTable(connection, _tableName);

            Parallel.For(0, 10, i =>
            {
                IList<object> values = GetValues(dataHint: 42);
                int numberOfRows = 10 + i;

                // Insert a bunch of rows in to the table.
                int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

                using (SqlConnection sqlConnection = new SqlConnection(connection))
                {
                    sqlConnection.Open();

                    // Do a select * from the table and check on the first column of the first row for the expected value.
                    using (SqlCommand sqlCommand = new SqlCommand
                    (
                        cmdText: $"select CustomerId, FirstName, LastName from [{_tableName}] where CustomerId = @CustomerId",
                        connection: sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        if (DataTestUtility.EnclaveEnabled)
                        {
                            // Increase timeout for enclave-enabled server
                            sqlCommand.CommandTimeout = 60;
                        }


                        sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                        int customerId = -1;

                        if (isAsync)
                        {
                            Task<object> result = VerifyExecuteScalarAsync(sqlCommand);
                            customerId = (int)result.Result;
                        }
                        else
                        {
                            customerId = (int)sqlCommand.ExecuteScalar();
                        }

                        Assert.Equal((int)values[0], customerId);
                    }
                }
            });
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithIntegers))]
        public void TestSqlDataAdapterBatchUpdate(string connection, int numberofRows)
        {
            CleanUpTable(connection, _tableName);

            DataTable dataTable = CreateDataTable(tableName: _tableName, numberofRows: numberofRows);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                // Create a SqlDataAdapter.
                SqlDataAdapter adapter = CreateSqlDataAdapter(sqlConnection);

                // Execute the update.
                int rowsAffected = adapter.Update(dataTable);

                Assert.Equal(numberofRows, rowsAffected);

                if (numberofRows == 100)
                {
                    // Delete a row, add two new ones and update the table again to make sure reusing the commands is working properly.
                    int lastRowId = dataTable.Rows.Count;
                    lastRowId++;
                    dataTable.Rows.RemoveAt(1);

                    DataRow row = dataTable.NewRow();
                    row["CustomerId"] = 45 + lastRowId;
                    row["FirstName"] = string.Format(@"Microsoft{0}", lastRowId);
                    row["LastName"] = string.Format(@"Corporation{0}", lastRowId);
                    dataTable.Rows.Add(row);
                    lastRowId++;

                    row = dataTable.NewRow();
                    row["CustomerId"] = 45 + lastRowId;
                    row["FirstName"] = string.Format(@"Microsoft{0}", lastRowId);
                    row["LastName"] = string.Format(@"Corporation{0}", lastRowId);
                    dataTable.Rows.Add(row);

                    rowsAffected = adapter.Update(dataTable);
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestExecuteReader(string connection)
        {
            CleanUpTable(connection, _tableName);

            Parallel.For(0, 10, i =>
            {
                IList<object> values = GetValues(dataHint: 45 + i + 1);
                int numberOfRows = 10 + i;
                // Insert a bunch of rows in to the table.
                int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);
                Assert.True(numberOfRows == rowsAffected, "Two values failed");

                using (SqlConnection sqlConnection = new SqlConnection(connection))
                {
                    sqlConnection.Open();

                    // Update the set of rows that were inserted just now. And verify the rows affected as returned by ExecuteNonQuery.
                    using (SqlCommand sqlCommand = new SqlCommand(
                        cmdText: $"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName=@FirstName AND CustomerId=@CustomerId ",
                        connection: sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        if (DataTestUtility.EnclaveEnabled)
                        {
                            //Increas command time out to a minute for enclave-enabled server.
                            sqlCommand.CommandTimeout = 60;
                        }

                        sqlCommand.Parameters.AddWithValue(@"FirstName", string.Format(@"Microsoft{0}", i + 100));
                        sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                        IAsyncResult asyncResult = sqlCommand.BeginExecuteReader();
                        Assert.True(asyncResult != null, "asyncResult should not be null");

                        using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                        {
                            while (sqlDataReader.Read())
                            {
                                Assert.Equal(sqlDataReader.GetInt32(0), (int)values[0]);
                                Assert.Equal(sqlDataReader.GetInt32(1), (int)values[1]);
                                Assert.Equal(sqlDataReader.GetInt32(2), (int)values[2]);
                            }

                            Assert.True(rowsAffected == numberOfRows, "no: of rows returned by EndExecuteReader is unexpected.");
                            Assert.True(3 == sqlDataReader.VisibleFieldCount);
                            Assert.True(3 == sqlDataReader.FieldCount);
                        }
                    }
                }
            });
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public async void TestExecuteReaderAsyncWithLargeQuery(string connectionString)
        {
            string randomName = DataTestUtility.GetUniqueName(Guid.NewGuid().ToString().Replace("-", ""), false);
            if (randomName.Length > 50)
            {
                randomName = randomName.Substring(0, 50);
            }
            string tableName = $"VeryLong_{randomName}_TestTableName";
            int columnsCount = 50;

            // Arrange - drops the table with long name and re-creates it with 52 columns (ID, name, ColumnName0..49)
            try
            {
                CreateTable(connectionString, tableName, columnsCount);
                string name = "nobody";

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    // This creates a "select top 100" query that has over 40k characters
                    using (SqlCommand sqlCommand = new SqlCommand(GenerateSelectQuery(tableName, columnsCount, 10, "WHERE Name = @FirstName AND ID = @CustomerId"),
                        connection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                        sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.VarChar, name.Length);

                        sqlCommand.Parameters[0].Value = 0;
                        sqlCommand.Parameters[1].Value = name;

                        // Act and Assert
                        // Test that execute reader async does not throw an exception.
                        // The table is empty so there should be no results; however, the bug previously found is that it causes a TDS RPC exception on enclave.
                        using (SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync())
                        {
                            Assert.False(sqlDataReader.HasRows, "The table should be empty");
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                DropTableIfExists(connectionString, tableName);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithCommandBehaviorSet1))]
        public void TestExecuteReaderWithCommandBehavior(string connection, CommandBehavior commandBehavior)
        {
            string[] columnNames = new string[3] { "CustomerId", "FirstName", "LastName" };
            string[] columnOrdinals = new string[3] { @"0", @"1", @"2" };
            string[] dataTypeName = new string[3] { @"int", @"nvarchar", @"nvarchar" };
            string[] providerType = new string[3] { @"8", @"12", @"12" };
            string[] providerSpecificDataType = new string[3] { @"System.Data.SqlTypes.SqlInt32", @"System.Data.SqlTypes.SqlString", @"System.Data.SqlTypes.SqlString" };
            string[] dataType = new string[3] { @"System.Int32", @"System.String", @"System.String" };
            string[] columnSizes = new string[3] { @"4", @"50", @"50" };

            CleanUpTable(connection, _tableName);

            Parallel.For(0, 1, i =>
            {
                IList<object> values = GetValues(dataHint: 16 + i);
                Assert.False(values == null || values.Count < 3, @"values should not be null and count should be >= 3.");
                int numberOfRows = 10 + i;
                // Insert a bunch of rows in to the table.
                int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);
                Assert.Equal(rowsAffected, numberOfRows);

                using (SqlConnection sqlConnection = new SqlConnection(connection))
                {
                    sqlConnection.Open();

                    // select the set of rows that were inserted just now.
                    using (SqlCommand sqlCommand = new SqlCommand(
                        cmdText: $"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                        connection: sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);
                        sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);

                        IAsyncResult asyncResult = sqlCommand.BeginExecuteReader(commandBehavior);

                        Assert.False(asyncResult == null, "asyncResult should not be null.");

                        rowsAffected = 0;

                        using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                        {
                            if (commandBehavior == CommandBehavior.KeyInfo || commandBehavior == CommandBehavior.SchemaOnly)
                            {
                                DataTable dataTable = sqlDataReader.GetSchemaTable();
                                Assert.False(dataTable != null, "dataTable should not be null.");
                                int j = 0;
                                foreach (DataRow dataRow in dataTable.Rows)
                                {
                                    Assert.True(dataRow[dataTable.Columns[0]].ToString() == columnNames[j], "column name mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[1]].ToString() == columnOrdinals[j], "column ordinal mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[2]].ToString() == columnSizes[j], "column sizes mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[12]].ToString() == dataType[j], "data type mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[14]].ToString() == providerType[j], "provider type mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[23]].ToString() == providerSpecificDataType[j], "provider specific data type mismatch.");
                                    Assert.True(dataRow[dataTable.Columns[24]].ToString() == dataTypeName[j], "data type name mismatch.");
                                    j++;
                                }
                            }
                            while (sqlDataReader.Read())
                            {
                                rowsAffected++;
                                Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");

                                if (commandBehavior != CommandBehavior.SequentialAccess)
                                {
                                    Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
                                    Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
                                }
                                else
                                {
                                    char[] textValue = new char[((string)values[1]).Length];
                                    sqlDataReader.GetChars(1, 0, textValue, 0, textValue.Length);
                                    Assert.True(new string(textValue) == (string)values[1], @"Value returned by GetChars is unexpected.");

                                    textValue = new char[((string)values[2]).Length];
                                    sqlDataReader.GetChars(2, 0, textValue, 0, textValue.Length);
                                    Assert.True(new string(textValue) == (string)values[2], @"Value returned by GetChars is unexpected.");
                                }
                            }

                            Assert.True(3 == sqlDataReader.VisibleFieldCount, "value returned by sqlDataReader.VisibleFieldCount is unexpected.");
                            Assert.True(3 == sqlDataReader.FieldCount, "value returned by sqlDataReader.FieldCount is unexpected.");
                        }
                    }

                    // Based on the command behavior, verify the appropriate outcome.
                    switch (commandBehavior)
                    {
                        case CommandBehavior.CloseConnection:
                            Assert.True(sqlConnection.State.Equals(ConnectionState.Closed),
                                    "CommandBehavior.CloseConnection did not close the connection after command execution.");
                            break;

                        case CommandBehavior.SingleResult:
                        case CommandBehavior.SequentialAccess:
                            //Assert.True(rowsAffected == 1, "rowsAffected did not match the expected number of rows.");
                            Assert.True(sqlConnection.State.Equals(ConnectionState.Open),
                                "CommandBehavior.SingleResult or SequentialAccess closed the connection after command execution.");
                            break;

                        case CommandBehavior.SingleRow:
                            // Assert.True(rowsAffected == 1, "rowsAffected did not match the expected number of rows.");
                            Assert.True(sqlConnection.State == ConnectionState.Open,
                                "CommandBehavior.SingleRow closed the connection after command execution.");
                            break;

                        case CommandBehavior.SchemaOnly:
                        case CommandBehavior.KeyInfo:
                            Assert.True(rowsAffected == 0, "rowsAffected did not match the expected number of rows.");
                            Assert.True(sqlConnection.State == ConnectionState.Open,
                                                "CommandBehavior.KeyInfo or CommandBehavior.SchemaOnly closed the connection after command execution.");
                            break;

                        default:

                            break;
                    }
                }
            });
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestEnclaveStoredProceduresWithAndWithoutParameters(string connectionString)
        {
            using SqlConnection sqlConnection = new(connectionString);
            sqlConnection.Open();

            using SqlCommand sqlCommand = new("", sqlConnection, transaction: null,
                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled);

            string procWithoutParams = DataTestUtility.GetUniqueName("EnclaveWithoutParams", withBracket: false);
            string procWithParam = DataTestUtility.GetUniqueName("EnclaveWithParams", withBracket: false);

            try
            {
                sqlCommand.CommandText = $"CREATE PROCEDURE {procWithoutParams} AS SELECT FirstName, LastName  FROM [{_tableName}];";
                sqlCommand.ExecuteNonQuery();
                sqlCommand.CommandText = $"CREATE PROCEDURE {procWithParam} @id INT AS SELECT FirstName, LastName FROM [{_tableName}] WHERE CustomerId = @id";
                sqlCommand.ExecuteNonQuery();
                int expectedFields = 2;

                sqlCommand.CommandText = procWithoutParams;
                sqlCommand.CommandType = CommandType.StoredProcedure;
                using (SqlDataReader reader = sqlCommand.ExecuteReader())
                {
                    Assert.Equal(expectedFields, reader.VisibleFieldCount);
                }

                sqlCommand.CommandText = procWithParam;
                sqlCommand.CommandType = CommandType.StoredProcedure;
                Exception ex = Assert.Throws<SqlException>(() => sqlCommand.ExecuteReader());
                string expectedMsg = $"Procedure or function '{procWithParam}' expects parameter '@id', which was not supplied.";

                Assert.Equal(expectedMsg, ex.Message);

                sqlCommand.Parameters.AddWithValue("@id", 0);
                using (SqlDataReader reader = sqlCommand.ExecuteReader())
                {
                    Assert.Equal(expectedFields, reader.VisibleFieldCount);
                }
            }
            finally
            {
                DropHelperProcedures(new[] { procWithoutParams, procWithParam }, connectionString);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestPrepareWithExecuteNonQuery(string connection)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 52);

            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (SqlCommand sqlCommand =
                    new SqlCommand($"UPDATE [{_tableName}] SET LastName = @LastName WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar, ((string)values[2]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    sqlCommand.Prepare();

                    rowsAffected = -1;
                    rowsAffected = sqlCommand.ExecuteNonQuery();

                    sqlCommand.Parameters[0].Value = (int)values[0] + 1;
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    rowsAffected = -1;
                    rowsAffected = sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestAsyncWriteDelayWithExecuteNonQueryAsync(string connection)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 53);
            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");
            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            using (SqlConnection sqlconnection = new SqlConnection(connection))
            {
                sqlconnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand($"UPDATE [{_tableName}] SET LastName = @LastName WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlconnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar, ((string)values[2]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    sqlCommand.Prepare();

                    rowsAffected = -1;

                    CommandHelper.s_debugForceAsyncWriteDelay?.SetValue(null, 10000);

                    Task<int> executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                    rowsAffected = executeTask.Result;

                    Assert.True(rowsAffected == 10, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");

                    sqlCommand.Parameters[0].Value = (int)values[0] + 1;
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    rowsAffected = -1;
                    executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                    rowsAffected = executeTask.Result;
                    Assert.True(rowsAffected == 0, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");
                    CommandHelper.s_debugForceAsyncWriteDelay?.SetValue(null, 0);
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestAsyncWriteDelayWithExecuteReaderAsync(string connection)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 53);

            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand($"UPDATE [{_tableName}] SET LastName = @LastName WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    connection: sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar, ((string)values[2]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    sqlCommand.Prepare();
                    rowsAffected = -1;

                    CommandHelper.s_debugForceAsyncWriteDelay?.SetValue(null, 10000);

                    Task<int> executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                    rowsAffected = executeTask.Result;

                    Assert.True(rowsAffected == 10, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    rowsAffected = 0;
                    Task<SqlDataReader> executeReaderTask = sqlCommand.ExecuteReaderAsync();

                    using (SqlDataReader sqlDataReader = executeReaderTask.Result)
                    {
                        while (sqlDataReader.Read())
                        {
                            rowsAffected++;
                            Assert.False(sqlDataReader == null, @"sqlDataReader should not be null.");
                            Assert.True(values != null && values.Count >= 3, @"values should not be null and should be with atleast 3 elements.");
                            Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");
                            Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
                            Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
                        }
                    }
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestPrepareWithExecuteNonQueryAsync(string connection)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 53);

            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand(
                    $"UPDATE [{_tableName}] SET LastName = @LastName WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar, ((string)values[2]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    sqlCommand.Prepare();

                    rowsAffected = -1;
                    Task<int> executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                    rowsAffected = executeTask.Result;

                    Assert.True(rowsAffected == 10, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");

                    sqlCommand.Parameters[0].Value = (int)values[0] + 1;
                    sqlCommand.Parameters[1].Value = values[1];
                    sqlCommand.Parameters[2].Value = values[2];

                    rowsAffected = -1;
                    executeTask = VerifyExecuteNonQueryAsync(sqlCommand);
                    rowsAffected = executeTask.Result;

                    Assert.True(rowsAffected == 0, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithCommandBehaviorSet2))]
        public void TestPrepareWithExecuteReaderAsync(string connection, CommandBehavior commandBehavior)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 54);
            Assert.True(values != null && values.Count <= 3, @"values should not be null and count should be >= 3.");
            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand($"SELECT * FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    sqlCommand.Prepare();

                    rowsAffected = 0;
                    IAsyncResult asyncResult = sqlCommand.BeginExecuteReader(commandBehavior);

                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                    {
                        while (sqlDataReader.Read())
                        {
                            rowsAffected++;

                            Assert.True(values != null && values.Count >= 3, @"values should not be null and should be with atleast 3 elements.");
                            Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");
                            Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
                            Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
                        }
                    }
                    Assert.True(rowsAffected == 10, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");

                    sqlCommand.Parameters[0].Value = (int)values[0] + 1;
                    sqlCommand.Parameters[1].Value = values[1];

                    Task<int> executeTask = VerifyExecuteNonQueryAsync(sqlCommand);

                    rowsAffected = -1;
                    rowsAffected = executeTask.Result;

                    Assert.True(rowsAffected == -1, "Unexpected number of rows affected as returned by ExecuteNonQueryAsync.");
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestSqlDataReaderAPIs(string connection)
        {
            CleanUpTable(connection, _tableName);

            SqlCommandColumnEncryptionSetting value = SqlCommandColumnEncryptionSetting.Enabled;
            char[] textValue = null;
            int numberOfRows = 100;
            string commandTextForEncryptionDisabledResultSetOnly = @"SELECT CustomerId, FirstName, LastName/*, BinaryColumn, NvarcharMaxColumn */ FROM [{0}]";
            string commandTextForEncryptionEnabled = @"SELECT CustomerId, FirstName, LastName /*, BinaryColumn, NvarcharMaxColumn*/ FROM [{0}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId";

            IList<object> values = GetValues(dataHint: 55);
            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(string.Format(value == SqlCommandColumnEncryptionSetting.Enabled
                            ? commandTextForEncryptionEnabled : commandTextForEncryptionDisabledResultSetOnly, _tableName),
                            sqlConnection,
                            transaction: null,
                            columnEncryptionSetting: value))
                {
                    if (value == SqlCommandColumnEncryptionSetting.Enabled)
                    {
                        sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                        sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                        sqlCommand.Parameters[0].Value = values[0];
                        sqlCommand.Parameters[1].Value = values[1];
                    }

                    sqlCommand.Prepare();
                    rowsAffected = 0;
                    IAsyncResult asyncResult = sqlCommand.BeginExecuteReader();
                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                    {
                        Assert.True(sqlDataReader.GetName(0) == @"CustomerId", "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetName(1) == @"FirstName", "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetName(2) == @"LastName", "LastName value read from the table was incorrect.");

                        Assert.True(sqlDataReader.GetOrdinal(@"CustomerId") == 0, "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetOrdinal(@"FirstName") == 1, "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetOrdinal(@"LastName") == 2, "LastName value read from the table was incorrect.");

                        VerifyDataTypes(sqlDataReader, value);

                        while (sqlDataReader.Read())
                        {
                            using (TextReader textReader = sqlDataReader.GetTextReader(1))
                            {
                                textValue = new char[((string)values[1]).Length];
                                textReader.Read(textValue, 0, textValue.Length);

                                Assert.True(new string(textValue) == (string)values[1], @"string value read through GetTextReader is unexpected.");
                            }

                            textValue = new char[((string)values[2]).Length];
                            sqlDataReader.GetChars(2, 0, textValue, 0, textValue.Length);

                            Assert.True(new string(textValue) == (string)values[2], @"Value returned by GetChars is unexpected.");

                            // GetFieldValue<T>
                            Assert.True(sqlDataReader.GetFieldValue<int>(0) == (int)values[0], @"Value returned by GetFieldValue is unexpected.");
                            Assert.True(sqlDataReader.GetFieldValue<string>(1) == (string)values[1], @"Value returned by GetFieldValue is unexpected.");
                            Assert.True(sqlDataReader.GetFieldValue<string>(2) == (string)values[2], @"Value returned by GetFieldValue is unexpected.");

                            // GetFieldValueAsync<T>
                            Task<int> getCustomerIdTask = sqlDataReader.GetFieldValueAsync<int>(0);
                            Assert.True(getCustomerIdTask.Result == (int)values[0], @"Value returned by GetFieldValueAsync is unexpected.");

                            Task<string> getFirstNameTask = sqlDataReader.GetFieldValueAsync<string>(1);
                            Assert.True(getFirstNameTask.Result == (string)values[1], @"Value returned by GetFieldValueAsync is unexpected.");

                            Task<string> getLastNameTask = sqlDataReader.GetFieldValueAsync<string>(2);
                            Assert.True(getLastNameTask.Result == (string)values[2], @"Value returned by GetFieldValueAsync is unexpected.");

                            // GetValues
                            object[] readValues = new object[values.Count];
                            int numberofValuesRead = sqlDataReader.GetValues(readValues);

                            Assert.True(numberofValuesRead == values.Count, "the number of values returned by GetValues is unexpected.");

                            // GetSqlValue
                            Assert.True(((System.Data.SqlTypes.SqlInt32)sqlDataReader.GetSqlValue(0)).Value == (int)values[0],
                                                @"Value returned by GetSqlValue is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)sqlDataReader.GetSqlValue(1)).Value == (string)values[1],
                                                @"Value returned by GetSqlValue is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)sqlDataReader.GetSqlValue(2)).Value == (string)values[2],
                                                @"Value returned by GetSqlValue is unexpected.");

                            // GetSqlValues
                            readValues = new object[values.Count];
                            numberofValuesRead = sqlDataReader.GetSqlValues(readValues);

                            Assert.True(numberofValuesRead == (int)values.Count, "the number of values returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlInt32)readValues[0]).Value == (int)values[0],
                                                 @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[1]).Value == (string)values[1],
                                                 @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[2]).Value == (string)values[2],
                                                 @"Value returned by GetSqlValues is unexpected.");

                            // IsDBNull
                            Assert.True(sqlDataReader.IsDBNull(0) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(1) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(2) == false, @"IsDBNull unexpectedly returned false.");

                            // IsDBNullAsync
                            Task<bool> isDbNullTask = sqlDataReader.IsDBNullAsync(0);

                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(1);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(2);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");
                        }

                        Assert.True(3 == sqlDataReader.VisibleFieldCount, "value returned by sqlDataReader.VisibleFieldCount is unexpected.");
                        Assert.True(3 == sqlDataReader.FieldCount, "value returned by sqlDataReader.FieldCount is unexpected.");
                    }
                }

                using (SqlCommand sqlCommand = new SqlCommand($"INSERT INTO [{_tableName}] VALUES (@CustomerId, @FirstName, @LastName /*, @BinaryColumn, @NvarcharMaxColumn*/)",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", 60);
                    SqlParameter firstNameParameter = new SqlParameter(@"FirstName", System.Data.SqlDbType2.NVarChar, 50);

                    firstNameParameter.Direction = System.Data.ParameterDirection.Input;
                    firstNameParameter.Value = DBNull.Value;

                    sqlCommand.Parameters.Add(firstNameParameter);
                    sqlCommand.Parameters.AddWithValue(@"LastName", @"Corporation60");

                    sqlCommand.ExecuteNonQuery();
                }

                using (SqlCommand sqlCommand =
                            new SqlCommand($"SELECT * FROM [{_tableName}] WHERE LastName = @LastName AND CustomerId = @CustomerId",
                            sqlConnection,
                            transaction: null,
                            columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int).Value = 60;
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar).Value = @"Corporation60";

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        while (sqlDataReader.Read())
                        {
                            // IsDBNull
                            Assert.True(sqlDataReader.IsDBNull(0) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(1) == true, @"IsDBNull unexpectedly returned true.");
                            Assert.True(sqlDataReader.IsDBNull(2) == false, @"IsDBNull unexpectedly returned false.");

                            // IsDBNullAsync
                            Task<bool> isDbNullTask = sqlDataReader.IsDBNullAsync(0);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(1);
                            Assert.True(isDbNullTask.Result == true, @"IsDBNullAsync unexpectedly returned true.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(2);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");
                        }
                    }
                }

                using (SqlCommand sqlCommand =
                           new SqlCommand($"UPDATE [{_tableName}] SET FirstName = @FirstName WHERE CustomerId = @CustomerId",
                           sqlConnection,
                           transaction: null,
                           columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"FirstName", string.Format(@"Microsoft{0}", values[1]));
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        Assert.Equal(sqlDataReader.RecordsAffected, numberOfRows);
                    }
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestSqlDataReaderAPIsWithSequentialAccess(string connection)
        {
            CleanUpTable(connection, _tableName);

            CommandBehavior value = CommandBehavior.SequentialAccess;
            char[] textValue = null;
            CommandBehavior commandBehavior = (CommandBehavior)value;

            IList<object> values = GetValues(dataHint: 56);

            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.Equal(rowsAffected, numberOfRows);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    sqlCommand.Prepare();
                    rowsAffected = 0;

                    IAsyncResult asyncResult = sqlCommand.BeginExecuteReader(commandBehavior);
                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                    {
                        Assert.True(sqlDataReader.GetName(0) == @"CustomerId", "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetName(1) == @"FirstName", "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetName(2) == @"LastName", "LastName value read from the table was incorrect.");

                        Assert.True(sqlDataReader.GetOrdinal(@"CustomerId") == 0, "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetOrdinal(@"FirstName") == 1, "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetOrdinal(@"LastName") == 2, "LastName value read from the table was incorrect.");

                        Assert.True(sqlDataReader.GetFieldType(0) == typeof(System.Int32), "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetFieldType(1) == typeof(System.String), "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetFieldType(2) == typeof(System.String), "LastName value read from the table was incorrect.");

                        Assert.True(sqlDataReader.GetProviderSpecificFieldType(0) == typeof(System.Data.SqlTypes.SqlInt32), "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetProviderSpecificFieldType(1) == typeof(System.Data.SqlTypes.SqlString), "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetProviderSpecificFieldType(2) == typeof(System.Data.SqlTypes.SqlString), "LastName value read from the table was incorrect.");

                        Assert.True(sqlDataReader.GetDataTypeName(0) == @"int", "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetDataTypeName(1) == @"nvarchar", "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetDataTypeName(2) == @"nvarchar", "LastName value read from the table was incorrect.");

                        while (sqlDataReader.Read())
                        {
                            textValue = new char[((string)values[1]).Length];
                            sqlDataReader.GetChars(1, 0, textValue, 0, textValue.Length);
                            Assert.True(new string(textValue) == (string)values[1], @"Value returned by GetChars is unexpected.");

                            textValue = new char[((string)values[2]).Length];
                            sqlDataReader.GetChars(2, 0, textValue, 0, textValue.Length);
                            Assert.True(new string(textValue) == (string)values[2], @"Value returned by GetChars is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            Exception ex = Assert.Throws<InvalidOperationException>(() => sqlDataReader.GetTextReader(1));
                            Assert.Equal("Retrieving encrypted column 'FirstName' with CommandBehavior=SequentialAccess is not supported.", ex.Message);
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand = new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // GetFieldValue<T>
                            Assert.True(sqlDataReader.GetFieldValue<int>(0) == (int)values[0], @"Value returned by GetFieldValue is unexpected.");
                            Assert.True(sqlDataReader.GetFieldValue<string>(1) == (string)values[1], @"Value returned by GetFieldValue is unexpected.");
                            Assert.True(sqlDataReader.GetFieldValue<string>(2) == (string)values[2], @"Value returned by GetFieldValue is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                   new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                   sqlConnection, transaction: null, columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // GetSqlValue
                            Assert.True(((System.Data.SqlTypes.SqlInt32)sqlDataReader.GetSqlValue(0)).Value == (int)values[0],
                                                @"Value returned by GetSqlValue is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)sqlDataReader.GetSqlValue(1)).Value == (string)values[1],
                                                 @"Value returned by GetSqlValue is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)sqlDataReader.GetSqlValue(2)).Value == (string)values[2],
                                                @"Value returned by GetSqlValue is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            object[] readValues = new object[values.Count];
                            int numberofValuesRead = sqlDataReader.GetValues(readValues);

                            Assert.True(numberofValuesRead == values.Count, "the number of values returned by GetValues is unexpected.");

                            for (int i = 0; i < numberofValuesRead; i++)
                            {

                                if (i != 3)
                                {
                                    Assert.True(readValues[i].ToString() == values[i].ToString(), @"the values returned by GetValues is unexpected.");
                                }
                                else
                                {
                                    Assert.True(((byte[])values[i]).SequenceEqual<byte>((byte[])readValues[i]),
                                            @"Value returned by GetValues is unexpected.");
                                }
                            }
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                new SqlCommand(
                    $"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // GetSqlValues
                            object[] readValues = new object[values.Count];
                            int numberofValuesRead = sqlDataReader.GetSqlValues(readValues);

                            Assert.True(numberofValuesRead == values.Count, "the number of values returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlInt32)readValues[0]).Value == (int)values[0],
                                                @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[1]).Value == (string)values[1],
                                                @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[2]).Value == (string)values[2],
                                                @"Value returned by GetSqlValues is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // GetSqlValues
                            object[] readValues = new object[values.Count];
                            int numberofValuesRead = sqlDataReader.GetSqlValues(readValues);

                            Assert.True(numberofValuesRead == values.Count, "the number of values returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlInt32)readValues[0]).Value == (int)values[0],
                                                @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[1]).Value == (string)values[1],
                                                @"Value returned by GetSqlValues is unexpected.");
                            Assert.True(((System.Data.SqlTypes.SqlString)readValues[2]).Value == (string)values[2],
                                @"Value returned by GetSqlValues is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                    new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection, transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // IsDBNullAsync
                            Task<bool> isDbNullTask = sqlDataReader.IsDBNullAsync(0);

                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(1);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(2);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                    new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection, transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // GetFieldValueAsync<T>
                            Task<int> getCustomerIdTask = sqlDataReader.GetFieldValueAsync<int>(0);
                            Assert.True(getCustomerIdTask.Result == (int)values[0], @"Value returned by GetFieldValueAsync is unexpected.");

                            Task<string> getFirstNameTask = sqlDataReader.GetFieldValueAsync<string>(1);
                            Assert.True(getFirstNameTask.Result == (string)values[1], @"Value returned by GetFieldValueAsync is unexpected.");

                            Task<string> getLastNameTask = sqlDataReader.GetFieldValueAsync<string>(2);
                            Assert.True(getLastNameTask.Result == (string)values[2], @"Value returned by GetFieldValueAsync is unexpected.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                    new SqlCommand($"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection, transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // IsDBNull
                            Assert.True(sqlDataReader.IsDBNull(0) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(1) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(2) == false, @"IsDBNull unexpectedly returned false.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                        new SqlCommand($"SELECT * FROM [{_tableName}] WHERE LastName = @LastName AND CustomerId = @CustomerId",
                        sqlConnection, transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int).Value = values[0];
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar).Value = values[2];

                    Task readAsyncTask = ReadAsync(sqlCommand, values, commandBehavior);
                    readAsyncTask.Wait();
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                        new SqlCommand($"UPDATE [{_tableName}] SET FirstName = @FirstName WHERE CustomerId = @CustomerId",
                        sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        Assert.True(sqlDataReader.RecordsAffected == numberOfRows, @"number of rows returned by sqlDataReader.RecordsAffected is incorrect.");
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand = new SqlCommand($"INSERT INTO [{_tableName}] VALUES (@CustomerId, @FirstName, @LastName )",
                    sqlConnection, transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", 60);
                    SqlParameter firstNameParameter = new SqlParameter(@"FirstName", System.Data.SqlDbType2.NVarChar, 50);

                    firstNameParameter.Direction = System.Data.ParameterDirection.Input;
                    firstNameParameter.Value = DBNull.Value;

                    sqlCommand.Parameters.Add(firstNameParameter);
                    sqlCommand.Parameters.AddWithValue(@"LastName", @"Corporation60");
                    sqlCommand.ExecuteNonQuery();
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                        new SqlCommand($"SELECT * FROM [{_tableName}] WHERE LastName = @LastName AND CustomerId = @CustomerId",
                        sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int).Value = 60;
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar).Value = @"Corporation60";

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // IsDBNull
                            Assert.True(sqlDataReader.IsDBNull(0) == false, @"IsDBNull unexpectedly returned false.");
                            Assert.True(sqlDataReader.IsDBNull(1) == true, @"IsDBNull unexpectedly returned true.");
                            Assert.True(sqlDataReader.IsDBNull(2) == false, @"IsDBNull unexpectedly returned false.");
                        }
                    }
                }

                // We use different commands for every API test, since SequentialAccess does not let you access a column more than once.
                using (SqlCommand sqlCommand =
                        new SqlCommand($"SELECT * FROM [{_tableName}] WHERE LastName = @LastName AND CustomerId = @CustomerId",
                        sqlConnection,
                        transaction: null,
                        columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int).Value = 60;
                    sqlCommand.Parameters.Add(@"LastName", SqlDbType2.NVarChar).Value = @"Corporation60";

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(commandBehavior))
                    {
                        while (sqlDataReader.Read())
                        {
                            // IsDBNullAsync
                            Task<bool> isDbNullTask = sqlDataReader.IsDBNullAsync(0);

                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(1);
                            Assert.True(isDbNullTask.Result == true, @"IsDBNullAsync unexpectedly returned true.");

                            isDbNullTask = sqlDataReader.IsDBNullAsync(2);
                            Assert.True(isDbNullTask.Result == false, @"IsDBNullAsync unexpectedly returned false.");
                        }
                    }
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithCommandBehaviorSet2))]
        public void TestSqlCommandSequentialAccessCodePaths(string connection, CommandBehavior value)
        {
            CleanUpTable(connection, _tableName);

            CommandBehavior commandBehavior = (CommandBehavior)value;
            IList<object> values = GetValues(dataHint: 57);

            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 100;

            //Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                // Test SqlDataReader.GetStream() on encrypted column, throw an exception.
                using (SqlCommand sqlCommand = new SqlCommand($"SELECT * FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    if (DataTestUtility.EnclaveEnabled)
                    {
                        //Increase Time out for enclave-enabled server.
                        sqlCommand.CommandTimeout = 90;
                    }

                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                    sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);

                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        Assert.True(sqlDataReader.VisibleFieldCount == values.Count, @"sqlDataReader.VisibleFieldCount returned unexpected result.");
                    }
                }
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestExecuteXmlReader(string connection)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 60);
            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.	
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);
            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                // select the set of rows that were inserted just now.	
                using (SqlCommand sqlCommand = new SqlCommand($"SELECT LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId FOR XML AUTO;", sqlConnection, transaction: null, columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    if (DataTestUtility.EnclaveEnabled)
                    {
                        //Increase Time out for enclave-enabled server.	
                        sqlCommand.CommandTimeout = 90;
                    }
                    sqlCommand.Parameters.Add(@"CustomerId", SqlDbType2.Int);
                    sqlCommand.Parameters.Add(@"FirstName", SqlDbType2.NVarChar, ((string)values[1]).Length);

                    sqlCommand.Parameters[0].Value = values[0];
                    sqlCommand.Parameters[1].Value = values[1];

                    sqlCommand.Prepare();
                    rowsAffected = 0;

                    var ex = Assert.Throws<SqlException>(() => sqlCommand.ExecuteXmlReader());
                    Assert.Equal($"'FOR XML' clause is unsupported for encrypted columns.{Environment.NewLine}Statement(s) could not be prepared.", ex.Message);
                }
            }
        }

        [ActiveIssue("10620")] // Randomly hangs the process.
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringSetupForAE))]
        [ClassData(typeof(AEConnectionStringProviderWithCommandBehaviorSet1))]
        public void TestBeginAndEndExecuteReaderWithAsyncCallback(string connection, CommandBehavior commandbehavior)
        {
            CleanUpTable(connection, _tableName);

            var test = commandbehavior;
            IList<object> values = GetValues(dataHint: 51);
            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 10;
            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);
            Assert.Equal(rowsAffected, numberOfRows);

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT * FROM {_tableName} WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);

                    TaskCompletionSource<object> completion = new TaskCompletionSource<object>();
                    IAsyncResult asyncResult = sqlCommand.BeginExecuteReader(new AsyncCallback(EndExecuteReaderAsyncCallBack),
                        stateObject: new TestAsyncCallBackStateObject(sqlCommand, expectedRowsAffected: commandbehavior == CommandBehavior.SingleRow ? 1 : numberOfRows,
                        completion: completion, expectedValues: values, commandBehavior: commandbehavior),
                        behavior: commandbehavior);
                    Assert.True(asyncResult != null, "asyncResult should not be null.");
                }
            }
        }

        [SkipOnTargetFramework(TargetFrameworkMonikers.Netcoreapp)]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringSetupForAE))]
        [ClassData(typeof(AEConnectionStringProviderWithExecutionMethod))]
        public void TestSqlCommandCancel(string connection, string value, int number)
        {
            CleanUpTable(connection, _tableName);

            string executeMethod = value;
            Assert.True(!string.IsNullOrWhiteSpace(executeMethod), @"executeMethod should not be null or empty");

            int numberOfCancelCalls = number;
            Assert.True(numberOfCancelCalls >= 0, "numberofCancelCalls should be >=0.");

            IList<object> values = GetValues(dataHint: 58);
            Assert.True(values != null && values.Count >= 3, @"values should not be null and count should be >= 3.");

            int numberOfRows = 300;
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);
            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT * FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                    sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);

                    CommandHelper.s_sleepDuringTryFetchInputParameterEncryptionInfo?.SetValue(null, true);

                    Thread[] threads = new Thread[2];

                    // Invoke ExecuteReader or ExecuteNonQuery in another thread.
                    if (executeMethod == @"ExecuteReader")
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteReader));
                    }
                    else
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteNonQuery));
                    }

                    threads[1] = new Thread(new ParameterizedThreadStart(Thread_Cancel));

                    // Start the execute thread.
                    threads[0].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Start the thread which cancels the above command started by the execute thread.
                    threads[1].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Wait for the threads to finish.
                    threads[0].Join();
                    threads[1].Join();

                    CommandHelper.s_sleepDuringTryFetchInputParameterEncryptionInfo?.SetValue(null, false);

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

                    rowsAffected = 0;

                    // See that we can still use the command object for running queries.
                    IAsyncResult asyncResult = sqlCommand.BeginExecuteReader();
                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                    {
                        while (sqlDataReader.Read())
                        {
                            rowsAffected++;
                            VerifyData(sqlDataReader, values);
                        }
                    }

                    Assert.True(rowsAffected == numberOfRows, "Unexpected number of rows affected as returned by EndExecuteReader.");

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

                    CommandHelper.s_sleepDuringRunExecuteReaderTdsForSpDescribeParameterEncryption?.SetValue(null, true);

                    // Invoke ExecuteReader or ExecuteNonQuery in another thread.
                    threads = new Thread[2];
                    if (executeMethod == @"ExecuteReader")
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteReader));
                    }
                    else
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteNonQuery));
                    }
                    threads[1] = new Thread(new ParameterizedThreadStart(Thread_Cancel));

                    // Start the execute thread.
                    threads[0].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Start the thread which cancels the above command started by the execute thread.
                    threads[1].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Wait for the threads to finish.
                    threads[0].Join();
                    threads[1].Join();

                    CommandHelper.s_sleepDuringRunExecuteReaderTdsForSpDescribeParameterEncryption?.SetValue(null, false);

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

                    rowsAffected = 0;

                    // See that we can still use the command object for running queries.
                    IAsyncResult asyncResult1 = sqlCommand.BeginExecuteReader(CommandBehavior.Default);
                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult1))
                    {
                        while (sqlDataReader.Read())
                        {
                            rowsAffected++;
                            VerifyData(sqlDataReader, values);
                        }
                    }

                    Assert.True(rowsAffected == numberOfRows, "Unexpected number of rows affected as returned by EndExecuteReader.");

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

                    CommandHelper.s_sleepAfterReadDescribeEncryptionParameterResults?.SetValue(null, true);

                    threads = new Thread[2];
                    if (executeMethod == @"ExecuteReader")
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteReader));
                    }
                    else
                    {
                        threads[0] = new Thread(new ParameterizedThreadStart(Thread_ExecuteNonQuery));
                    }
                    threads[1] = new Thread(new ParameterizedThreadStart(Thread_Cancel));

                    // Start the execute thread.
                    threads[0].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Start the thread which cancels the above command started by the execute thread.
                    threads[1].Start(new TestCommandCancelParams(sqlCommand, _tableName, numberOfCancelCalls));

                    // Wait for the threads to finish.
                    threads[0].Join();
                    threads[1].Join();

                    CommandHelper.s_sleepAfterReadDescribeEncryptionParameterResults?.SetValue(null, false);

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

                    rowsAffected = 0;

                    // See that we can still use the command object for running queries.
                    asyncResult = sqlCommand.BeginExecuteReader(CommandBehavior.Default);
                    using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
                    {
                        while (sqlDataReader.Read())
                        {
                            rowsAffected++;
                            VerifyData(sqlDataReader, values);
                        }
                    }

                    Assert.True(rowsAffected == numberOfRows, "Unexpected number of rows affected as returned by EndExecuteReader.");

                    // Verify the state of the sql command object.
                    VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);
                };
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProviderWithCancellationTime))]
        public void TestSqlCommandCancellationToken(string connection, int initalValue, int cancellationTime)
        {
            CleanUpTable(connection, _tableName);

            IList<object> values = GetValues(dataHint: 59);
            int numberOfRows = 10;

            // Insert a bunch of rows in to the table.
            int rowsAffected = InsertRows(tableName: _tableName, numberofRows: numberOfRows, values: values, connection: connection);

            Assert.True(rowsAffected == numberOfRows, "number of rows affected is unexpected.");

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (SqlCommand sqlCommand = new SqlCommand($@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE FirstName = @FirstName AND CustomerId = @CustomerId",
                    connection: sqlConnection,
                    transaction: null,
                    columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                {
                    sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                    sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);

                    // Simulate a sleep during TryFetchInputParameterEncryptionInfo.
                    TestCancellationToken(CommandHelper.s_sleepDuringTryFetchInputParameterEncryptionInfo,
                        sqlCommand,
                        numberOfRows,
                        values,
                        commandType: initalValue,
                        cancelAfter: cancellationTime);

                    TestCancellationToken(CommandHelper.s_sleepAfterReadDescribeEncryptionParameterResults,
                        sqlCommand,
                        numberOfRows,
                        values,
                        commandType: initalValue,
                        cancelAfter: cancellationTime);

                    TestCancellationToken(CommandHelper.s_sleepDuringRunExecuteReaderTdsForSpDescribeParameterEncryption,
                       sqlCommand,
                       numberOfRows,
                       values,
                       commandType: initalValue,
                       cancelAfter: cancellationTime);
                }
            }
        }


        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsSGXEnclaveConnStringSetup))]
        public void TestNoneAttestationProtocolWithSGXEnclave()
        {
            SqlConnectionStringBuilder builder = new(DataTestUtility.TCPConnectionStringAASSGX);
            builder.AttestationProtocol = SqlConnectionAttestationProtocol.None;
            builder.EnclaveAttestationUrl = string.Empty;

            using (SqlConnection connection = new(builder.ConnectionString))
            {
                InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => connection.Open());
                string expectedErrorMessage = string.Format(
                    SystemDataResourceManager.Instance.TCE_AttestationProtocolNotSupportEnclaveType,
                    SqlConnectionAttestationProtocol.None.ToString(), "SGX");
                Assert.Contains(expectedErrorMessage, ex.Message);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestConnectionCustomKeyStoreProviderDuringAeQuery(string connectionString)
        {
            if (!SQLSetupStrategyAzureKeyVault.IsAKVProviderRegistered)
            {
                SqlColumnEncryptionAzureKeyVaultProvider sqlColumnEncryptionAzureKeyVaultProvider =
                    new(new SqlClientCustomTokenCredential());
                SQLSetupStrategyAzureKeyVault.RegisterGlobalProviders(sqlColumnEncryptionAzureKeyVaultProvider);
            }

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();

                // will use DummyProvider in global cache
                // provider will be found but it will throw when its methods are called
                Exception ex = Assert.Throws<SqlException>(
                      () => ExecuteQueryThatRequiresCustomKeyStoreProvider(connection));
                AssertExceptionCausedByFailureToDecrypt(ex);

                // not required provider in instance cache
                // it should not fall back to the global cache so the right provider will not be found
                connection.RegisterColumnEncryptionKeyStoreProvidersOnConnection(_notRequiredProvider);
                ex = Assert.Throws<ArgumentException>(
                     () => ExecuteQueryThatRequiresCustomKeyStoreProvider(connection));
                Assert.Equal(_providerNotFoundMessage, ex.Message);

                // required provider in instance cache
                // if the instance cache is not empty, it is always checked for the provider.
                // => if the provider is found, it must have been retrieved from the instance cache and not the global cache
                connection.RegisterColumnEncryptionKeyStoreProvidersOnConnection(_requiredProvider);
                ex = Assert.Throws<SqlException>(
                    () => ExecuteQueryThatRequiresCustomKeyStoreProvider(connection));
                AssertExceptionCausedByFailureToDecrypt(ex);

                // not required provider will replace the previous entry so required provider will not be found 
                connection.RegisterColumnEncryptionKeyStoreProvidersOnConnection(_notRequiredProvider);
                ex = Assert.Throws<ArgumentException>(
                    () => ExecuteQueryThatRequiresCustomKeyStoreProvider(connection));
                Assert.Equal(_providerNotFoundMessage, ex.Message);
            }

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();

                // new connection instance should have an empty cache and query will fall back to global cache
                // which contains the required provider
                Exception ex = Assert.Throws<SqlException>(
                      () => ExecuteQueryThatRequiresCustomKeyStoreProvider(connection));
                AssertExceptionCausedByFailureToDecrypt(ex);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringSetupForAE), nameof(DataTestUtility.IsAKVSetupAvailable))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestCommandCustomKeyStoreProviderDuringAeQuery(string connectionString)
        {
            if (!SQLSetupStrategyAzureKeyVault.IsAKVProviderRegistered)
            {
                SqlColumnEncryptionAzureKeyVaultProvider sqlColumnEncryptionAzureKeyVaultProvider =
                    new(new SqlClientCustomTokenCredential());
                SQLSetupStrategyAzureKeyVault.RegisterGlobalProviders(sqlColumnEncryptionAzureKeyVaultProvider);
            }

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();
                using (SqlCommand command = CreateCommandThatRequiresCustomKeyStoreProvider(connection))
                {
                    // will use DummyProvider in global cache
                    // provider will be found but it will throw when its methods are called
                    Exception ex = Assert.Throws<SqlException>(() => command.ExecuteReader());
                    AssertExceptionCausedByFailureToDecrypt(ex);

                    // required provider will be found in command cache
                    command.RegisterColumnEncryptionKeyStoreProvidersOnCommand(_requiredProvider);
                    ex = Assert.Throws<SqlException>(() => command.ExecuteReader());
                    AssertExceptionCausedByFailureToDecrypt(ex);

                    // not required provider in command cache
                    command.RegisterColumnEncryptionKeyStoreProvidersOnCommand(_notRequiredProvider);
                    ex = Assert.Throws<ArgumentException>(() => command.ExecuteReader());
                    Assert.Equal(_providerNotFoundMessage, ex.Message);

                    // not required provider in command cache, required provider in connection cache
                    // should not fall back to connection cache or global cache
                    connection.RegisterColumnEncryptionKeyStoreProvidersOnConnection(_requiredProvider);
                    ex = Assert.Throws<ArgumentException>(() => command.ExecuteReader());
                    Assert.Equal(_providerNotFoundMessage, ex.Message);

                    using (SqlCommand command2 = CreateCommandThatRequiresCustomKeyStoreProvider(connection))
                    {
                        // new command instance should have an empty cache and query will fall back to connection cache
                        // which contains the required provider
                        ex = Assert.Throws<SqlException>(() => command2.ExecuteReader());
                        AssertExceptionCausedByFailureToDecrypt(ex);
                    }
                }
            }
        }

        // On Windows, "_fixture" will be type SQLSetupStrategyCertStoreProvider
        // On non-Windows, "_fixture" will be type SQLSetupStrategyAzureKeyVault
        // Test will pass on both but only SQLSetupStrategyCertStoreProvider is a system provider
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsTargetReadyForAeWithKeyStore))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestSystemProvidersHavePrecedenceOverInstanceLevelProviders(string connectionString)
        {
            Dictionary<string, SqlColumnEncryptionKeyStoreProvider> customKeyStoreProviders = new()
            {
                {
                    SqlColumnEncryptionAzureKeyVaultProvider.ProviderName,
                    new SqlColumnEncryptionAzureKeyVaultProvider(new SqlClientCustomTokenCredential())
                }
            };

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();
                using SqlCommand command = CreateCommandThatRequiresSystemKeyStoreProvider(connection);
                connection.RegisterColumnEncryptionKeyStoreProvidersOnConnection(customKeyStoreProviders);
                SqlDataReader reader = command.ExecuteReader();
                Assert.Equal(3, reader.VisibleFieldCount);
            }

            using (SqlConnection connection = new(connectionString))
            {
                connection.Open();
                using SqlCommand command = CreateCommandThatRequiresSystemKeyStoreProvider(connection);
                command.RegisterColumnEncryptionKeyStoreProvidersOnCommand(customKeyStoreProviders);
                SqlDataReader reader = command.ExecuteReader();
                Assert.Equal(3, reader.VisibleFieldCount);
            }
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringSetupForAE), nameof(DataTestUtility.EnclaveEnabled))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestRetryWhenAEParameterMetadataCacheIsStale(string connectionString)
        {
            CleanUpTable(connectionString, _tableName);

            const int customerId = 50;
            IList<object> values = GetValues(dataHint: customerId);
            InsertRows(tableName: _tableName, numberofRows: 1, values: values, connection: connectionString);

            ApiTestTable table = _fixture.ApiTestTable as ApiTestTable;
            string enclaveSelectQuery = $@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE CustomerId > @CustomerId";
            string alterCekQueryFormatString = "ALTER TABLE [{0}] " +
                "ALTER COLUMN [CustomerId] [int]  " +
                "ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{1}], " +
                "ENCRYPTION_TYPE = Randomized, " +
                "ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'); " +
                "ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;";

            using SqlConnection sqlConnection = new(connectionString);
            sqlConnection.Open();

            // execute the select query to add its parameter metadata and enclave-required CEKs to the cache
            using SqlCommand cmd = new SqlCommand(enclaveSelectQuery, sqlConnection, null, SqlCommandColumnEncryptionSetting.Enabled);
            cmd.Parameters.AddWithValue("CustomerId", 0);
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Equal(customerId, (int)reader[0]);
                }
                reader.Close();
            };

            // change the CEK for the CustomerId column from ColumnEncryptionKey1 to ColumnEncryptionKey2
            // this will render the select query's cache entry stale
            cmd.Parameters.Clear();
            cmd.CommandText = string.Format(alterCekQueryFormatString, _tableName, table.columnEncryptionKey2.Name);
            cmd.ExecuteNonQuery();

            // execute the select query again. it will attempt to use the stale cache entry, receive 
            // a retryable error from the server, remove the stale cache entry, retry and succeed
            cmd.CommandText = enclaveSelectQuery;
            cmd.Parameters.AddWithValue("@CustomerId", 0);
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Equal(customerId, (int)reader[0]);
                }
                reader.Close();
            }

            // revert the CEK change to the CustomerId column
            cmd.Parameters.Clear();
            cmd.CommandText = string.Format(alterCekQueryFormatString, _tableName, table.columnEncryptionKey1.Name);
            cmd.ExecuteNonQuery();
        }

        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringSetupForAE), nameof(DataTestUtility.EnclaveEnabled))]
        [ClassData(typeof(AEConnectionStringProvider))]
        public void TestRetryWhenAEEnclaveCacheIsStale(string connectionString)
        {
            CleanUpTable(connectionString, _tableName);

            const int customerId = 50;
            IList<object> values = GetValues(dataHint: customerId);
            InsertRows(tableName: _tableName, numberofRows: 1, values: values, connection: connectionString);

            ApiTestTable table = _fixture.ApiTestTable as ApiTestTable;
            string enclaveSelectQuery = $@"SELECT CustomerId, FirstName, LastName FROM [{_tableName}] WHERE CustomerId > @CustomerId";
            string alterCekQueryFormatString = "ALTER TABLE [{0}] " +
                "ALTER COLUMN [CustomerId] [int]  " +
                "ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{1}], " +
                "ENCRYPTION_TYPE = Randomized, " +
                "ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'); " +
                "ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;";

            using SqlConnection sqlConnection = new(connectionString);
            sqlConnection.Open();

            // change the CEK and encryption type to randomized for the CustomerId column to ensure enclaves are used
            using SqlCommand cmd = new SqlCommand(
                string.Format(alterCekQueryFormatString, _tableName, table.columnEncryptionKey2.Name),
                sqlConnection,
                null,
                SqlCommandColumnEncryptionSetting.Enabled);
            cmd.ExecuteNonQuery();

            // execute the select query to create the cache entry
            cmd.CommandText = enclaveSelectQuery;
            cmd.Parameters.AddWithValue("@CustomerId", 0);
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Equal(customerId, (int)reader[0]);
                }
                reader.Close();
            }

            CommandHelper.InvalidateEnclaveSession(cmd);

            // Execute again to exercise the session retry logic
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Equal(customerId, (int)reader[0]);
                }
                reader.Close();
            }

            CommandHelper.InvalidateEnclaveSession(cmd);

            // Execute again to exercise the async session retry logic
            Task readAsyncTask = ReadAsync(cmd, values, CommandBehavior.Default);
            readAsyncTask.GetAwaiter().GetResult();

#if DEBUG
            CommandHelper.ForceThrowDuringGenerateEnclavePackage(cmd);

            // Execute again to exercise the session retry logic
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Equal(customerId, (int)reader[0]);
                }
                reader.Close();
            }

            CommandHelper.ForceThrowDuringGenerateEnclavePackage(cmd);

            // Execute again to exercise the async session retry logic
            Task readAsyncTask2 = ReadAsync(cmd, values, CommandBehavior.Default);
            readAsyncTask2.GetAwaiter().GetResult();
#endif

            // revert the CEK change to the CustomerId column
            cmd.Parameters.Clear();
            cmd.CommandText = string.Format(alterCekQueryFormatString, _tableName, table.columnEncryptionKey1.Name);
            cmd.ExecuteNonQuery();
        }

        private void ExecuteQueryThatRequiresCustomKeyStoreProvider(SqlConnection connection)
        {
            using (SqlCommand command = CreateCommandThatRequiresCustomKeyStoreProvider(connection))
            {
                command.ExecuteReader();
            }
        }

        private SqlCommand CreateCommandThatRequiresCustomKeyStoreProvider(SqlConnection connection)
        {
            SqlCommand command = new(
                $"SELECT * FROM [{_fixture.CustomKeyStoreProviderTestTable.Name}] WHERE CustomerID = @id",
                connection, null, SqlCommandColumnEncryptionSetting.Enabled);
            command.Parameters.AddWithValue("id", 9);
            return command;
        }

        private SqlCommand CreateCommandThatRequiresSystemKeyStoreProvider(SqlConnection connection)
        {
            SqlCommand command = new(
                    $"SELECT * FROM [{_fixture.CustomKeyStoreProviderTestTable.Name}] WHERE FirstName = @firstName",
                    connection, null, SqlCommandColumnEncryptionSetting.Enabled);
            command.Parameters.AddWithValue("firstName", "abc");
            return command;
        }

        private void AssertExceptionCausedByFailureToDecrypt(Exception ex)
        {
            Assert.Contains(_failedToDecryptMessage, ex.Message);
            Assert.True(ex.InnerException is NotImplementedException);
        }

        private SqlDataAdapter CreateSqlDataAdapter(SqlConnection sqlConnection)
        {
            // Create a SqlDataAdapter.
            SqlDataAdapter adapter = new SqlDataAdapter(string.Empty, sqlConnection)
            {

                // Set the SELECT command.
                SelectCommand = new SqlCommand
            (
                cmdText: $"SELECT CustomerId, FirstName, LastName  FROM [{_tableName}]",
                connection: sqlConnection,
                transaction: null,
                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled
            ),

                // Set the UPDATE command and parameters.
                UpdateCommand = new SqlCommand
            (
                cmdText: $"UPDATE [{_tableName}] SET FirstName=@FirstName WHERE CustomerId=@CustomerId",
                connection: sqlConnection,
                transaction: null,
                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled
            )
            };
            adapter.UpdateCommand.Parameters.Add("@FirstName", SqlDbType2.NVarChar, 50, "FirstName");
            adapter.UpdateCommand.Parameters.Add("@CustomerId", SqlDbType2.Int, 4, "CustomerId");
            adapter.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;

            // Set the INSERT command and parameter.
            adapter.InsertCommand = new SqlCommand
            (
                cmdText: $"INSERT INTO [{_tableName}] (FirstName, LastName) VALUES (@FirstName, @LastName);",
                connection: sqlConnection,
                transaction: null,
                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled
            );
            adapter.InsertCommand.Parameters.Add("@FirstName", SqlDbType2.NVarChar, 50, "FirstName");
            adapter.InsertCommand.Parameters.Add("@LastName", SqlDbType2.NVarChar, 50, "LastName");

            adapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;

            // Set the DELETE command and parameter.
            adapter.DeleteCommand = new SqlCommand(
                cmdText: $"DELETE FROM [{_tableName}] WHERE CustomerId=@CustomerId",
                connection: sqlConnection,
                transaction: null,
                columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled
            );
            adapter.DeleteCommand.Parameters.Add("@CustomerId", SqlDbType2.Int, 4, "CustomerId");
            adapter.DeleteCommand.UpdatedRowSource = UpdateRowSource.None;

            // Set the batch size.
            adapter.UpdateBatchSize = 10;
            return adapter;
        }

        /// <summary>
        /// Create a data table.
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDataTable(string tableName, int numberofRows)
        {
            // Create a new DataTable.
            DataTable table = new DataTable(tableName);

            // Declare variables for DataColumn and DataRow objects.
            DataColumn column;
            DataRow row;

            column = new DataColumn
            {
                DataType = System.Type.GetType("System.Int32"),
                ColumnName = "CustomerId",
                ReadOnly = false,
                Unique = false
            };
            table.Columns.Add(column);

            // Create second column.
            column = new DataColumn
            {
                DataType = System.Type.GetType("System.String"),
                ColumnName = "FirstName",
                ReadOnly = false,
                Unique = false
            };
            table.Columns.Add(column);

            // Create third column.
            column = new DataColumn
            {
                DataType = System.Type.GetType("System.String"),
                ColumnName = "LastName",
                ReadOnly = false,
                Unique = false
            };
            table.Columns.Add(column);

            // Create three new DataRow objects and add  
            // them to the DataTable 
            for (int i = 0; i < numberofRows; i++)
            {
                row = table.NewRow();
                row["CustomerId"] = 45 + i + 1;
                row["FirstName"] = string.Format(@"Microsoft{0}", i);
                row["LastName"] = string.Format(@"Corporation{0}", i);

                table.Rows.Add(row);
            }

            return table;
        }

        /// <summary>
        /// Insert rows in to the table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="numberofRows"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        private int InsertRows(string tableName, int numberofRows, IList<object> values, string connection)
        {
            int rowsAffected = 0;

            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();

                // Temporarily inserting one row at a time, since server can't support multiple rows insert yet.
                for (int i = 0; i < numberofRows; i++)
                {
                    using (SqlCommand sqlCommand = new SqlCommand("", sqlConnection, transaction: null, columnEncryptionSetting: SqlCommandColumnEncryptionSetting.Enabled))
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{tableName}] VALUES (@CustomerId, @FirstName, @LastName)";
                        sqlCommand.Parameters.AddWithValue(@"CustomerId", values[0]);
                        sqlCommand.Parameters.AddWithValue(@"FirstName", values[1]);
                        sqlCommand.Parameters.AddWithValue(@"LastName", values[2]);
                        rowsAffected += sqlCommand.ExecuteNonQuery();
                    }
                }
            }
            return rowsAffected;
        }

        /// <summary>
        /// Drops the specified procedures.
        /// </summary>
        /// <param name="procNames"></param>
        private void DropHelperProcedures(string[] procNames, string connection)
        {
            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                foreach (string name in procNames)
                {
                    string procedureName = name.Trim(new Char[] { '[', ']' });

                    using (SqlCommand cmd = new SqlCommand(string.Format("IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{0}') \n DROP PROCEDURE {0}", procedureName), sqlConnection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private async Task<string> ReadAsync(SqlCommand sqlCommand, IList<object> values, CommandBehavior commandBehavior)
        {
            Assert.True(sqlCommand != null, @"sqlCommand should not be null.");
            Assert.True(values != null && values.Count >= 3, @"values should not be null and values.count should be >= 3.");

            string xmlResult = null;

            using (SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync(commandBehavior))
            {
                while (await sqlDataReader.ReadAsync())
                {
                    Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");
                    Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
                    Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
                }
            }
            return xmlResult;
        }

        /// <summary>
        /// Asynchronously invoke ExecuteNonQuery using await.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        private async Task<int> VerifyExecuteNonQueryAsync(SqlCommand sqlCommand)
        {
            int rowsAffected = await sqlCommand.ExecuteNonQueryAsync();

            return rowsAffected;
        }

        /// <summary>
        /// Asynchronously invoke ExecuteScalar using await.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        private async Task<object> VerifyExecuteScalarAsync(SqlCommand sqlCommand)
        {
            object result = await sqlCommand.ExecuteScalarAsync();

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task ExecuteNonQueryAsync(SqlCommand sqlCommand, CancellationToken cancellationToken)
        {
            await sqlCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        private async Task<SqlDataReader> ExecuteReaderAsync(SqlCommand sqlCommand)
        {
            return await sqlCommand.ExecuteReaderAsync();
        }

        /// <summary>
        /// ExecuteScalarAsync with CancellationToken
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="cancellationToken"></param>
        private async Task ExecuteScalarAsync(SqlCommand sqlCommand, CancellationToken cancellationToken)
        {
            await sqlCommand.ExecuteScalarAsync(cancellationToken);
        }

        /// <summary>
        /// ExecuteReaderAsync with CancellationToken
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="cancellationToken"></param>
        private async Task ExecuteReaderAsync(SqlCommand sqlCommand, CancellationToken cancellationToken)
        {
            using (SqlDataReader reader = await sqlCommand.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync())
                { }
            }
        }

        /// <summary>
        /// Verify the state of the sqlcommand object to be in the expected state after completion or cancellation.
        /// </summary>
        /// <param name="sqlCommand"></param>
        private void VerifySqlCommandStateAfterCompletionOrCancel(SqlCommand sqlCommand)
        {
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");

            // Assert the state of sqlCommand variables after the cancel.
            bool isDescribeParameterEncryptionInProgress =
               (bool)CommandHelper.s_isDescribeParameterEncryptionRPCCurrentlyInProgress?.GetValue(sqlCommand);

            object sqlParameterEncryptionTCEArray = CommandHelper.s_sqlRPCParameterEncryptionReqArray?.GetValue(sqlCommand);
            Assert.True(sqlParameterEncryptionTCEArray == null, @"sqlParameterEncryptionTCEArray should be null.");

            int currentlyExecutingDescribeParameterEncryptionRPC =
                (int)CommandHelper.s_currentlyExecutingDescribeParameterEncryptionRPC?.GetValue(sqlCommand);
            Assert.True(currentlyExecutingDescribeParameterEncryptionRPC == 0, @"currentlyExecutingDescribeParameterEncryptionRPC should be 0.");

            int rowsAffectedBySpDescribeParameterEncryption =
                (int)CommandHelper.s_rowsAffectedBySpDescribeParameterEncryption?.GetValue(sqlCommand);
            Assert.True(rowsAffectedBySpDescribeParameterEncryption == -1, @"rowsAffectedBySpDescribeParameterEncryption should be -1.");
        }

        /// <summary>
        /// Populate a list of Values for the predefined set of default columns.
        /// </summary>
        /// <param name="dataHint"></param>
        /// <returns></returns>
        private IList<object> GetValues(int dataHint)
        {
            IList<object> values = new List<object>(3)
            {
                dataHint,
                string.Format("Microsoft{0}", dataHint),
                string.Format("Corporation{0}", dataHint)
            };

            return values;
        }

        private void VerifyData(SqlDataReader sqlDataReader, IList<object> values)
        {
            Assert.True(sqlDataReader != null, @"sqlDataReader should not be null.");
            Assert.True(values != null && values.Count >= 3, @"values should not be null and should be with atleast 3 elements.");

            Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");
            Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
            Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
        }

        /// <summary>
        /// Verify data types of the columns as obtained by ExecuteReader.
        /// </summary>
        /// <param name="sqlDataReader"></param>
        /// <param name="columnEncryptionSetting"></param>
        /// <returns></returns>
        private void VerifyDataTypes(SqlDataReader sqlDataReader, SqlCommandColumnEncryptionSetting columnEncryptionSetting)
        {
            Assert.False(sqlDataReader == null, @"sqlDataReader should not be null.");

            if (columnEncryptionSetting != SqlCommandColumnEncryptionSetting.Disabled)
            {
                Assert.True(sqlDataReader.GetFieldType(0) == typeof(System.Int32), "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetFieldType(1) == typeof(System.String), "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetFieldType(2) == typeof(System.String), "LastName value read from the table was incorrect.");

                Assert.True(sqlDataReader.GetProviderSpecificFieldType(0) == typeof(System.Data.SqlTypes.SqlInt32), "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetProviderSpecificFieldType(1) == typeof(System.Data.SqlTypes.SqlString), "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetProviderSpecificFieldType(2) == typeof(System.Data.SqlTypes.SqlString), "LastName value read from the table was incorrect.");

                Assert.True(sqlDataReader.GetDataTypeName(0) == @"int", "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetDataTypeName(1) == @"nvarchar", "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetDataTypeName(2) == @"nvarchar", "LastName value read from the table was incorrect.");
            }
            else
            {
                Assert.True(sqlDataReader.GetFieldType(0) == typeof(System.Byte[]), "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetFieldType(1) == typeof(System.Byte[]), "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetFieldType(2) == typeof(System.Byte[]), "LastName value read from the table was incorrect.");

                Assert.True(sqlDataReader.GetProviderSpecificFieldType(0) == typeof(System.Data.SqlTypes.SqlBinary), "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetProviderSpecificFieldType(1) == typeof(System.Data.SqlTypes.SqlBinary), "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetProviderSpecificFieldType(2) == typeof(System.Data.SqlTypes.SqlBinary), "LastName value read from the table was incorrect.");

                Assert.True(sqlDataReader.GetDataTypeName(0) == @"varbinary", "CustomerId value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetDataTypeName(1) == @"varbinary", "FirstName value read from the table was incorrect.");
                Assert.True(sqlDataReader.GetDataTypeName(2) == @"varbinary", "LastName value read from the table was incorrect.");
            }
        }

        private void EndExecuteReaderAsyncCallBack(IAsyncResult asyncResult)
        {
            TestAsyncCallBackStateObject testAsyncCallBackStateObject = (TestAsyncCallBackStateObject)asyncResult.AsyncState;
            Assert.True(testAsyncCallBackStateObject != null, "testAsyncCallBackStateObject should not be null.");
            Assert.True(testAsyncCallBackStateObject.SqlCommand != null, "testAsyncCallBackStateObject.SqlCommand should not be null.");
            Assert.True(testAsyncCallBackStateObject.Values != null, "testAsyncCallBackStateObject.Values should not be null.");

            int rowsAffected = 0;

            try
            {
                using (SqlDataReader sqlDataReader = testAsyncCallBackStateObject.SqlCommand.EndExecuteReader(asyncResult))
                {
                    while (sqlDataReader.Read())
                    {
                        rowsAffected++;
                        Assert.True(sqlDataReader.GetInt32(0) == (int)testAsyncCallBackStateObject.Values[0], "CustomerId value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetString(1) == (string)testAsyncCallBackStateObject.Values[1], "FirstName value read from the table was incorrect.");
                        Assert.True(sqlDataReader.GetString(2) == (string)testAsyncCallBackStateObject.Values[2], "LastName value read from the table was incorrect.");
                    }

                    Assert.True(3 == sqlDataReader.VisibleFieldCount, "value returned by sqlDataReader.VisibleFieldCount is unexpected.");
                    Assert.True(3 == sqlDataReader.FieldCount, "value returned by sqlDataReader.FieldCount is unexpected.");
                }

                // Based on the command behavior, verify the appropriate outcome.
                switch (testAsyncCallBackStateObject.Behavior)
                {
                    case CommandBehavior.CloseConnection:
                        Assert.True(rowsAffected == testAsyncCallBackStateObject.ExpectedRowsAffected, "rowsAffected did not match the expected number of rows.");
                        Assert.True(testAsyncCallBackStateObject.SqlCommand.Connection.State == ConnectionState.Closed,
                            "CommandBehavior.CloseConnection did not close the connection after command execution.");
                        break;

                    case CommandBehavior.SingleResult:
                    case CommandBehavior.Default:
                    case CommandBehavior.SequentialAccess:
                        Assert.True(rowsAffected == testAsyncCallBackStateObject.ExpectedRowsAffected, "rowsAffected did not match the expected number of rows.");
                        Assert.True(testAsyncCallBackStateObject.SqlCommand.Connection.State == ConnectionState.Open,
                            "CommandBehavior.SingleResult or SequentialAccess closed the connection after command execution.");
                        break;

                    case CommandBehavior.SingleRow:
                        Assert.True(rowsAffected == 1, "rowsAffected did not match the expected number of rows.");
                        Assert.True(testAsyncCallBackStateObject.SqlCommand.Connection.State == ConnectionState.Open,
                            "CommandBehavior.SingleRow closed the connection after command execution.");
                        break;

                    case CommandBehavior.SchemaOnly:
                    case CommandBehavior.KeyInfo:
                        Assert.True(rowsAffected == 0, "rowsAffected did not match the expected number of rows.");
                        Assert.True(testAsyncCallBackStateObject.SqlCommand.Connection.State == ConnectionState.Open,
                                            "CommandBehavior.KeyInfo or CommandBehavior.SchemaOnly closed the connection after command execution.");
                        break;

                    default:
                        Assert.True(false);
                        break;
                }

                Assert.True(rowsAffected == testAsyncCallBackStateObject.ExpectedRowsAffected,
                "expected rows affected does not match the actual rows as returned by EndExecuteReader with async callback option.");
            }
            catch (Exception e)
            {
                testAsyncCallBackStateObject.Completion.SetException(e);
                Assert.Fail($"{e.Message}");
            }
        }

        /// <summary>
        /// Test the results of DataAdapter.Fill command.
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="values"></param>
        private void TestDataAdapterFillResults(DataTable dataTable, IList<object> values)
        {
            foreach (DataRow row in dataTable.Rows)
            {
                Assert.Equal(values[0], row.ItemArray[0]);
                Assert.Equal(values[1], row.ItemArray[1]);
                Assert.Equal(values[2], row.ItemArray[2]);
            }
        }

        private void CleanUpTable(string connString, string tableName)
        {
            using (var sqlConnection = new SqlConnection(connString))
            {
                sqlConnection.Open();
                Table.DeleteData(tableName, sqlConnection);
            }
        }

        private static void CreateTable(string connString, string tableName, int columnsCount)
            => DataTestUtility.RunNonQuery(connString, GenerateCreateQuery(tableName, columnsCount));
        /// <summary>
        /// Drops the table if the specified table exists
        /// </summary>
        /// <param name="connString">The connection string to the database</param>
        /// <param name="tableName">The name of the table to be dropped</param>
        private static void DropTableIfExists(string connString, string tableName)
        {
            using var sqlConnection = new SqlConnection(connString);
            sqlConnection.Open();
            DataTestUtility.DropTable(sqlConnection, tableName);
        }

        /// <summary>
        /// Generates the query for creating a table with the number of bit columns specified.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columnsCount">The number of columns for the table</param>
        /// <returns></returns>
        private static string GenerateCreateQuery(string tableName, int columnsCount)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(string.Format("CREATE TABLE [dbo].[{0}]", tableName));
            builder.Append('(');
            builder.AppendLine("[ID][bigint] NOT NULL,");
            builder.AppendLine("[Name] [varchar] (200) NOT NULL");
            for (int i = 0; i < columnsCount; i++)
            {
                builder.Append(',');
                builder.Append($"[ColumnName{i}][bit] NULL");
            }
            builder.Append(");");

            return builder.ToString();
        }

        /// <summary>
        /// Generates the large query with the select top 100 of all the columns repeated multiple times.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columnsCount">The number of columns to be explicitly included</param>
        /// <param name="repeat">The number of times the select query is repeated</param>
        /// <param name="where">A where clause for additional filters</param>
        /// <returns></returns>
        private static string GenerateSelectQuery(string tableName, int columnsCount, int repeat = 10, string where = "")
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine($"SELECT TOP 100");
            builder.AppendLine($"[{tableName}].[ID],");
            builder.AppendLine($"[{tableName}].[Name]");
            for (int i = 0; i < columnsCount; i++)
            {
                builder.Append(",");
                builder.AppendLine($"[{tableName}].[ColumnName{i}]");
            }

            string extra = string.IsNullOrEmpty(where) ? $"(NOLOCK) [{tableName}]" : where;
            builder.AppendLine($"FROM [{tableName}] {extra};");

            StringBuilder builder2 = new StringBuilder();
            for (int i = 0; i < repeat; i++)
            {
                builder2.AppendLine(builder.ToString());
            }

            return builder2.ToString();
        }

        /// <summary>
        /// An helper method to test the cancellation of the command using cancellationToken to async SqlCommand APIs.
        /// </summary>
        /// <param name="failpoint"></param>
        /// <param name="sqlCommand"></param>
        /// <param name="numberOfRows"></param>
        /// <param name="values"></param>
        /// <param name="commandType"></param>
        private void TestCancellationToken(FieldInfo failpoint, SqlCommand sqlCommand, int numberOfRows, IList<object> values, int commandType, int cancelAfter)
        {
            int rowsAffected = 0;
            sqlCommand.CommandTimeout = 0;
            VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

            // Enable the failpoint which will make the task sleep during PrepareForTransparentEncryption.
            failpoint?.SetValue(null, value: true);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(millisecondsDelay: cancelAfter);

            Task executeReaderAsyncTask = null;

            switch (commandType)
            {
                case 0:
                    executeReaderAsyncTask = ExecuteReaderAsync(sqlCommand, cancellationTokenSource.Token);
                    break;

                case 1:
                    executeReaderAsyncTask = ExecuteNonQueryAsync(sqlCommand, cancellationTokenSource.Token);
                    break;

                case 2:
                    executeReaderAsyncTask = ExecuteScalarAsync(sqlCommand, cancellationTokenSource.Token);
                    break;
            }

            Assert.True(executeReaderAsyncTask != null, "executeReaderAsyncTask should not be null.");

            // Cancel the command.
            try
            {
                executeReaderAsyncTask.Wait();
            }
            catch (AggregateException aggregateException)
            {
                foreach (Exception ex in aggregateException.InnerExceptions)
                {
                    Assert.True(ex is SqlException, @"cancelling a command through cancellation token resulted in unexpected exception.");
                    Assert.True(@"Operation cancelled by user." == ex.Message, @"cancelling a command through cancellation token resulted in unexpected error message.");
                }
            }

            failpoint?.SetValue(null, value: false);

            VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

            rowsAffected = 0;

            // See that we can still use the command object for running queries.
            IAsyncResult asyncResult = sqlCommand.BeginExecuteReader(CommandBehavior.Default);
            using (SqlDataReader sqlDataReader = sqlCommand.EndExecuteReader(asyncResult))
            {
                while (sqlDataReader.Read())
                {
                    rowsAffected++;

                    Assert.True(sqlDataReader.GetInt32(0) == (int)values[0], "CustomerId value read from the table was incorrect.");
                    Assert.True(sqlDataReader.GetString(1) == (string)values[1], "FirstName value read from the table was incorrect.");
                    Assert.True(sqlDataReader.GetString(2) == (string)values[2], "LastName value read from the table was incorrect.");
                }

                Assert.True(3 == sqlDataReader.VisibleFieldCount, "value returned by sqlDataReader.VisibleFieldCount is unexpected.");
                Assert.True(3 == sqlDataReader.FieldCount, "value returned by sqlDataReader.FieldCount is unexpected.");
            }

            VerifySqlCommandStateAfterCompletionOrCancel(sqlCommand);

            Assert.True(rowsAffected == numberOfRows, "Unexpected number of rows affected as returned by EndExecuteReader.");
        }

        private void Thread_ExecuteReader(object cancelCommandTestParamsObject)
        {
            Assert.True(cancelCommandTestParamsObject != null, @"cancelCommandTestParamsObject should not be null.");
            SqlCommand sqlCommand = ((TestCommandCancelParams)cancelCommandTestParamsObject).SqlCommand as SqlCommand;
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");

            string.Format(@"SELECT * FROM {0} WHERE FirstName = @FirstName AND CustomerId = @CustomerId", ((TestCommandCancelParams)cancelCommandTestParamsObject).TableName);
            using (SqlDataReader reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    Assert.Throws<InvalidOperationException>(() => sqlCommand.ExecuteReader());
                }
            }
        }

        private void Thread_ExecuteNonQuery(object cancelCommandTestParamsObject)
        {
            Assert.True(cancelCommandTestParamsObject != null, @"cancelCommandTestParamsObject should not be null.");
            SqlCommand sqlCommand = ((TestCommandCancelParams)cancelCommandTestParamsObject).SqlCommand as SqlCommand;
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");

            string.Format(@"UPDATE {0} SET FirstName = @FirstName WHERE FirstName = @FirstName AND CustomerId = @CustomerId", ((TestCommandCancelParams)cancelCommandTestParamsObject).TableName);

            Exception ex = Assert.Throws<InvalidOperationException>(() => sqlCommand.ExecuteNonQuery());
            Assert.Equal(@"Operation cancelled by user.", ex.Message);
        }

        private void Thread_Cancel(object cancelCommandTestParamsObject)
        {
            Assert.True(cancelCommandTestParamsObject != null, @"cancelCommandTestParamsObject should not be null.");
            SqlCommand sqlCommand = ((TestCommandCancelParams)cancelCommandTestParamsObject).SqlCommand as SqlCommand;
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");

            Thread.Sleep(millisecondsTimeout: 500);

            // Repeatedly cancel.
            for (int i = 0; i < ((TestCommandCancelParams)cancelCommandTestParamsObject).NumberofTimesToRunCancel; i++)
            {
                sqlCommand.Cancel();
            }
        }

        public void Dispose()
        {
            foreach (string connection in DataTestUtility.AEConnStringsSetup)
            {
                using (SqlConnection sqlConnection = new SqlConnection(connection))
                {
                    sqlConnection.Open();

                    Table.DeleteData(_fixture.ApiTestTable.Name, sqlConnection);
                }
            }
        }
    }

    public struct Customer
    {
        public Customer(int id, string firstName, string lastName)
        {
            Id = id;
            FirstName = firstName;
            LastName = lastName;
        }

        public int Id { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }
    }

#if NET6_0_OR_GREATER
    public record CustomerDateOnly(int Id, string FirstName, string LastName, DateOnly DateOfBirth, TimeOnly TimeOfDay);
#endif

    internal class TestAsyncCallBackStateObject
    {
        /// <summary>
        /// SqlCommand object.
        /// </summary>
        private readonly SqlCommand _sqlCommand;

        /// <summary>
        /// Number of rows expected to be affected.
        /// </summary>
        private readonly int _expectedRowsAffected;

        /// <summary>
        /// List of objects representing values.
        /// </summary>
        private readonly IList<object> _expectedValues;

        /// <summary>
        /// Command Behavior.
        /// </summary>
        private readonly CommandBehavior _commandBehavior;

        /// <summary>
        /// Return the sqlcommand.
        /// </summary>
        internal SqlCommand SqlCommand
        {
            get
            {
                return _sqlCommand;
            }
        }

        /// <summary>
        /// Return the expected number of rows affected.
        /// </summary>
        internal int ExpectedRowsAffected
        {
            get
            {
                return _expectedRowsAffected;
            }
        }

        /// <summary>
        /// Return the values list.
        /// </summary>
        internal IList<object> Values
        {
            get
            {
                return _expectedValues;
            }
        }

        /// <summary>
        /// Return CommandBehavior.
        /// </summary>
        internal CommandBehavior Behavior
        {
            get
            {
                return _commandBehavior;
            }
        }

        internal TaskCompletionSource<object> Completion { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="expectedRowsAffected"></param>
        /// <param name="values"></param>
        internal TestAsyncCallBackStateObject(SqlCommand sqlCommand, int expectedRowsAffected, IList<object> expectedValues, TaskCompletionSource<object> completion,
            CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");
            Assert.True(expectedRowsAffected >= 0 || expectedValues != null, "expectedRowsAffected should be >= 0 or values should not be null.");

            _sqlCommand = sqlCommand;
            _expectedRowsAffected = expectedRowsAffected;
            _expectedValues = expectedValues;
            _commandBehavior = commandBehavior;
            Completion = completion;
        }
    }

    internal class TestCommandCancelParams
    {
        /// <summary>
        /// SqlCommand object.
        /// </summary>
        private readonly object _sqlCommand;

        /// <summary>
        /// Name of the test/works as table name
        /// </summary>
        private readonly string _tableName;

        /// <summary>
        /// number of times to run cancel.
        /// </summary>
        private readonly int _numberofCancelCommands;

        /// <summary>
        /// Return the SqlCommand object.
        /// </summary>
        public object SqlCommand
        {
            get
            {
                return _sqlCommand;
            }
        }

        /// <summary>
        /// Return the tablename.
        /// </summary>
        public object TableName
        {
            get
            {
                return _tableName;
            }
        }

        /// <summary>
        /// Return the number of times to run cancel.
        /// </summary>
        public int NumberofTimesToRunCancel
        {
            get
            {
                return _numberofCancelCommands;
            }
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <param name="tableName"></param>
        /// <param name="numberofTimesToCancel"></param>
        public TestCommandCancelParams(object sqlCommand, string tableName, int numberofTimesToCancel)
        {
            Assert.True(sqlCommand != null, "sqlCommand should not be null.");
            Assert.True(!string.IsNullOrWhiteSpace(tableName), "tableName should not be null or empty.");

            _sqlCommand = sqlCommand;
            _tableName = tableName;
            _numberofCancelCommands = numberofTimesToCancel;
        }
    }
}
