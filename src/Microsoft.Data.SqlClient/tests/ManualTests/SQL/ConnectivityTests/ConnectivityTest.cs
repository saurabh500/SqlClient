// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Xunit;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests
{
    public static class ConnectivityTest
    {
        private const string COL_SPID = "SPID";
        private const string COL_PROGRAM_NAME = "ProgramName";
        private const string COL_HOSTNAME = "HostName";
        private static readonly string s_databaseName = "d_" + Guid.NewGuid().ToString().Replace('-', '_');
        private static readonly string s_tableName = DataTestUtility.GenerateObjectName();
        private static readonly string s_connectionString = DataTestUtility.TCPConnectionString;
        private static readonly string s_dbConnectionString = new SqlConnectionStringBuilder(s_connectionString) { InitialCatalog = s_databaseName }.ConnectionString;
        private static readonly string s_createDatabaseCmd = $"CREATE DATABASE {s_databaseName}";
        private static readonly string s_createTableCmd = $"CREATE TABLE {s_tableName} (NAME NVARCHAR(40), AGE INT)";
        private static readonly string s_alterDatabaseSingleCmd = $"ALTER DATABASE {s_databaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
        private static readonly string s_alterDatabaseMultiCmd = $"ALTER DATABASE {s_databaseName} SET MULTI_USER WITH ROLLBACK IMMEDIATE;";
        private static readonly string s_selectTableCmd = $"SELECT COUNT(*) FROM {s_tableName}";
        private static readonly string s_dropDatabaseCmd = $"DROP DATABASE {s_databaseName}";


        [Fact]
        public static void ConnectionTestRoundTrip()
        {
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true);
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = "localhost",
                InitialCatalog = "master",
                UserID = "sa",
                Encrypt = SqlConnectionEncryptOption.Strict,
                Password = "ss1234",
                Pooling = false
            };
            //for (int i = 0 ; i < 10; i++)
            {
                SqlConnection connection = new SqlConnection(builder.ConnectionString);
                
                connection.Open();
                string command = "select CAST('11111122222222222222222333333333333333333333333333333312312312312333333333333123' as binary(5500))";
                using (SqlCommand cmd = new SqlCommand(command, connection))
                {
                    
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine(BitConverter.ToString((byte[])reader[0]));
                        }
                    }
                }
                connection.Close();
                
            }
        }

        [Fact]
        public static void WriteFile()
        {
            byte[] data = new byte[500 * 1024 * 1024]; // 20 MB
            new Random().NextBytes(data); // Fill with random bytes
            
            string filePath = "C:\\temp\\LargeBinaryData.bin";
            File.WriteAllBytes(filePath, data);
        }

        [Fact]
        public static void ConnectionLargeDataInsert()
        {
            //string largeFilePath = "C:\\temp\\LargeTextData.txt"; // Ensure this file exists with large text data
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true);
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:localhost",
                InitialCatalog = "drivers",
                UserID = "sa",
                Encrypt = SqlConnectionEncryptOption.Optional,
                Password = "ss1234",
                Pooling = false
            };
            //for (int i = 0 ; i < 10; i++)
            using (SqlConnection conn = new SqlConnection(builder.ConnectionString))
            {
                conn.Open();
                byte[] data = new byte[500 * 1024 * 1024]; // 20 MB
                new Random().NextBytes(data); // Fill with random bytes


                string insertSql = "INSERT INTO dbo.LargeBinaryTable (binarydata) VALUES (@data)";


                using (SqlCommand cmd = new SqlCommand(insertSql, conn))
                {
                    // Open a TextReader for the large text file
                    //using (TextReader reader = new StreamReader(largeFilePath))
                    {
                        //SqlParameter param = new SqlParameter("@data", SqlDbType.NVarChar, -1);
                        //param.Value = reader;
                        //param.IsNullable = true;

                        cmd.Parameters.Add("@data", System.Data.SqlDbType.VarBinary, -1).Value = data;

                        // Use Structured Streaming API
                        //cmd.Parameters.Add(param);
                        cmd.ExecuteNonQuery();
                    }
                }
            }

        }

        [Fact]
        public static void ConnectionProcedure()
        {
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true);
            StringWriter stringWriter = new StringWriter((IFormatProvider)null);
            Console.WriteLine(stringWriter.Encoding);
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:172.20.107.10,14333",
                InitialCatalog = "drivers",
                UserID = "sa",
                Encrypt = SqlConnectionEncryptOption.Optional,
                Password = "ss1234",
                Pooling = false
            };
            //for (int i = 0 ; i < 10; i++)
            {
                SqlConnection connection = new SqlConnection(builder.ConnectionString);

                connection.Open();
                string command = "Geography";
                using (SqlCommand cmd = new SqlCommand(command, connection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlParameter parameter = new SqlParameter();
                    parameter.ParameterName = "@OutputPoint";
                    parameter.SqlDbType = System.Data.SqlDbType.Udt;
                    parameter.Direction = ParameterDirection.Output;
                    parameter.UdtTypeName = "geography"; // Ensure this UDT exists in your database
                    cmd.Parameters.Add(parameter);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            object value = reader.IsDBNull(0) ? "NULL" : reader.GetValue(0);
                            Console.WriteLine($"Value {value}");
                            Console.WriteLine(value);
                        }
                    }
                }
                connection.Close();
            }
        }
        [Fact]
        public static void AdhocStuff()
        {
            long v = 0xabcdef;
            int length = 8; // Number of bytes to extract

            byte[] manualBytes = new byte[length];
            for (int index = 0; index < length; index++)
            { 
                manualBytes[index] = (byte)((v >> (index * 8)) & 0xff);
            }
            var theSpan = new Span<byte>(new byte[8]);
            BinaryPrimitives.WriteInt64LittleEndian(theSpan, v);

            Assert.Equal(theSpan.Length, manualBytes.Length);
            for (int i = 0; i < theSpan.Length; i++)
            {
                Assert.Equal(theSpan[i], manualBytes[i]);
            }
        }
        [Fact]
        public static void ConnectionTvpTest()
        {
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true);
            StringWriter stringWriter = new StringWriter((IFormatProvider)null);
            Console.WriteLine(stringWriter.Encoding);
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = "djr4e56djc2uzhpd6wkdpvxupq-lmfnijwabysunflfa65ucbm4gm.database.fabric.microsoft.com",
                InitialCatalog = "dlevy_SalesLT-daa4a8fa-c8b5-4a7f-b2b8-17b611faba65",
                //UserID = "sa",
                Encrypt = SqlConnectionEncryptOption.Strict,
                Authentication = SqlAuthenticationMethod.ActiveDirectoryDefault,
                //Password = "ss1234",
                Pooling = false
            };
            //for (int i = 0 ; i < 10; i++)
            {
                SqlConnection connection = new SqlConnection(builder.ConnectionString);

                connection.Open();
                // Create a DataTable matching the TVP structure
                var tvpTable = new DataTable();
                tvpTable.Columns.Add("Id", typeof(int));
                tvpTable.Columns.Add("Name", typeof(string));

                // Populate DataTable with sample data
                tvpTable.Rows.Add(1, "Alice");
                tvpTable.Rows.Add(2, "Bob");

                using (var command = new SqlCommand("dbo.ViewTVP", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    var tvpParam = command.Parameters.AddWithValue("@Data", tvpTable);
                    tvpParam.SqlDbType = SqlDbType.Structured;
                    tvpParam.TypeName = "dbo.MyTableType";

                    using (var rdr = command.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            // Assuming the TVP returns two columns: Id and Name
                            int id = rdr.GetInt32(0);
                            string name = rdr.GetString(1);
                            Console.WriteLine($"Id: {id}, Name: {name}");
                        }
                    }
                }


                Console.WriteLine("TVP data inserted successfully.");
                connection.Close();
            }
        }

        [Fact]
        public static void ConnectionLargeData()
        {
            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true);
            StringWriter stringWriter = new StringWriter((IFormatProvider)null);
            Console.WriteLine(stringWriter.Encoding);
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:172.20.107.10",
                InitialCatalog = "drivers",
                UserID = "sa",
                Encrypt = SqlConnectionEncryptOption.Optional,
                Password = "ss1234",
                Pooling = false
            };
            //for (int i = 0 ; i < 10; i++)
            {
                SqlConnection connection = new SqlConnection(builder.ConnectionString);

                connection.Open();
                string command = "SELECT @something as something;";
                

                SqlParameter param = new SqlParameter();
                param.ParameterName = "@something";
                
                param.SqlDbType = SqlDbType.Date;
                var dt = DateTime.Now;
                //dt.Columns.Add("Column1", typeof(string));
                param.Value = DBNull.Value; // Assigning a sample DataTable value

                //param.Size = 9111;
                //param.Scale = 3;

                using (SqlCommand cmd = new SqlCommand(command, connection))
                {
                    cmd.Parameters.Add(param);
                    cmd.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        // Synapse: Stored procedure sp_who2 does not exist or is not supported.
        // Synapse: SqlConnection.ServerProcessId is always retrieved as 0.
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureSynapse))]
        public static void EnvironmentHostNameSPIDTest()
        {
            SqlConnectionStringBuilder builder = new(DataTestUtility.TCPConnectionString)
            {
                Pooling = true,
                ApplicationName = "HostNameTest"
            };

            using (SqlConnection sqlConnection = new(builder.ConnectionString))
            {
                sqlConnection.Open();
                int sqlClientSPID = sqlConnection.ServerProcessId;
                int sessionSpid;

                using (SqlCommand cmd = new("SELECT @@SPID", sqlConnection))
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    sessionSpid = reader.GetInt16(0);
                }

                // Confirm Server process id is same as result of SELECT @@SPID
                Assert.Equal(sessionSpid, sqlClientSPID);

                // Confirm once again SPID on SqlConnection directly
                Assert.Equal(sessionSpid, sqlConnection.ServerProcessId);

                using (SqlCommand command = new("sp_who2", sqlConnection))
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int programNameOrdinal = reader.GetOrdinal(COL_PROGRAM_NAME);
                        string programName = reader.GetString(programNameOrdinal);

                        int spidOrdinal = reader.GetOrdinal(COL_SPID);
                        string spid = reader.GetString(spidOrdinal);

                        if (programName != null && programName.Trim().Equals(builder.ApplicationName) && short.Parse(spid) == sessionSpid)
                        {
                            // Get the hostname
                            int hostnameOrdinal = reader.GetOrdinal(COL_HOSTNAME);
                            string hostnameFromServer = reader.GetString(hostnameOrdinal);
                            string expectedMachineName = Environment.MachineName.ToUpper();
                            string hostNameFromServer = hostnameFromServer.Trim().ToUpper();
                            Assert.Matches(expectedMachineName, hostNameFromServer);
                            return;
                        }
                    }
                }
                // Confirm Server Process Id stays the same after query execution
                Assert.Equal(sessionSpid, sqlConnection.ServerProcessId);
            }
            Assert.Fail("No non-empty hostname found for the application");
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup))]
        public static async Task ConnectionTimeoutInfiniteTest()
        {
            // Exercise the special-case infinite connect timeout code path
            SqlConnectionStringBuilder builder = new(DataTestUtility.TCPConnectionString)
            {
                ConnectTimeout = 0 // Infinite
            };

            using SqlConnection conn = new(builder.ConnectionString);
            CancellationTokenSource cts = new(30000);
            // Will throw TaskCanceledException and fail the test in the event of a hang
            await conn.OpenAsync(cts.Token);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup))]
        public static void ConnectionTimeoutTestWithThread()
        {
            const int timeoutSec = 5;
            const int numOfTry = 2;
            const int numOfThreads = 5;

            SqlConnectionStringBuilder builder = new(DataTestUtility.TCPConnectionString)
            {
                DataSource = "invalidhost",
                ConnectTimeout = timeoutSec
            };
            string connStrNotAvailable = builder.ConnectionString;

            for (int i = 0; i < numOfThreads; ++i)
            {
                new ConnectionWorker(connStrNotAvailable, numOfTry);
            }

            ConnectionWorker.Start();
            ConnectionWorker.Stop();

            double timeTotal = 0;

            foreach (ConnectionWorker w in ConnectionWorker.WorkerList)
            {
                timeTotal += w.TimeElapsed;
            }
            double timeElapsed = timeTotal / Convert.ToDouble(ConnectionWorker.WorkerList.Count);
            int threshold = timeoutSec * numOfTry * 2 * 1000;

            Assert.True(timeElapsed < threshold);
        }

        // Synapse: Catalog view 'sysprocesses' is not supported in this version.
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureSynapse))]
        public static void LocalProcessIdTest()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(DataTestUtility.TCPConnectionString);
            string sqlProviderName = builder.ApplicationName;
            string sqlProviderProcessID = Process.GetCurrentProcess().Id.ToString();

            using (SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString))
            {
                sqlConnection.Open();
                string strCommand = $"SELECT PROGRAM_NAME,HOSTPROCESS FROM SYS.SYSPROCESSES WHERE PROGRAM_NAME LIKE ('%{sqlProviderName}%')";
                using (SqlCommand command = new SqlCommand(strCommand, sqlConnection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        bool processIdFound = false;
                        while (reader.Read())
                        {
                            Assert.Equal(sqlProviderName, reader.GetString(0).Trim());
                            if (sqlProviderProcessID == reader.GetString(1).Trim())
                            {
                                processIdFound = true;
                            }
                        }
                        Assert.True(processIdFound);
                    }
                }
            }
        }
        public class ConnectionWorker : IDisposable
        {
            private static List<ConnectionWorker> s_workerList = new();
            private ManualResetEventSlim _doneEvent = new(false);
            private double _timeElapsed;
            private Thread _thread;
            private string _connectionString;
            private int _numOfTry;

            public ConnectionWorker(string connectionString, int numOfTry)
            {
                s_workerList.Add(this);
                _connectionString = connectionString;
                _numOfTry = numOfTry;
                _thread = new Thread(new ThreadStart(SqlConnectionOpen));
            }

            public static List<ConnectionWorker> WorkerList => s_workerList;

            public double TimeElapsed => _timeElapsed;

            public static void Start()
            {
                foreach (ConnectionWorker w in s_workerList)
                {
                    w._thread.Start();
                }
            }

            public static void Stop()
            {
                foreach (ConnectionWorker w in s_workerList)
                {
                    w._doneEvent.Wait();
                }
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _doneEvent.Dispose();
                }
            }

            public void SqlConnectionOpen()
            {
                Stopwatch sw = new Stopwatch();
                double totalTime = 0;
                for (int i = 0; i < _numOfTry; ++i)
                {
                    using (SqlConnection con = new(_connectionString))
                    {
                        sw.Start();
                        try
                        {
                            con.Open();
                        }
                        catch { }
                        sw.Stop();
                    }
                    totalTime += sw.Elapsed.TotalMilliseconds;
                    sw.Reset();
                }
                _timeElapsed = totalTime / Convert.ToDouble(_numOfTry);

                _doneEvent.Set();
            }
        }

        // Synapse: Parse error at line: 1, column: 59: Incorrect syntax near 'SINGLE_USER' - No support for MULTI_USER
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureServer))]
        public static void ConnectionKilledTest()
        {
            try
            {
                // Setup Database and Table.
                DataTestUtility.RunNonQuery(s_connectionString, s_createDatabaseCmd);
                DataTestUtility.RunNonQuery(s_dbConnectionString, s_createTableCmd);

                // Kill all the connections and set Database to SINGLE_USER Mode.
                DataTestUtility.RunNonQuery(s_connectionString, s_alterDatabaseSingleCmd, 4);
                // Set Database back to MULTI_USER Mode
                DataTestUtility.RunNonQuery(s_connectionString, s_alterDatabaseMultiCmd, 4);

                // Execute SELECT statement.
                DataTestUtility.RunNonQuery(s_dbConnectionString, s_selectTableCmd);
            }
            finally
            {
                // Kill all the connections, set Database to SINGLE_USER Mode and drop Database
                DataTestUtility.RunNonQuery(s_connectionString, s_alterDatabaseSingleCmd, 4);
                DataTestUtility.RunNonQuery(s_connectionString, s_dropDatabaseCmd, 4);
            }
        }

        // Synapse: KILL not supported on Azure Synapse - Parse error at line: 1, column: 6: Incorrect syntax near '105'.
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureSynapse))]
        public static void ConnectionResiliencySPIDTest()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(DataTestUtility.TCPConnectionString)
            {
                ConnectRetryCount = 0,
                ConnectRetryInterval = 5
            };

            // No connection resiliency
            using (SqlConnection conn = new(builder.ConnectionString))
            {
                conn.Open();
                InternalConnectionWrapper wrapper = new(conn, true, builder.ConnectionString);
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT @@SPID";
                wrapper.KillConnectionByTSql();
                bool cmdSuccess = false;
                try
                {
                    cmd.ExecuteScalar();
                    cmdSuccess = true;
                }
                // Windows always throws SqlException. Unix sometimes throws AggregateException against Azure SQL DB.
                catch (Exception ex) when (ex is SqlException || ex is AggregateException) { }
                Assert.False(cmdSuccess);
            }

            builder.ConnectRetryCount = 2;
            // Also check SPID changes with connection resiliency
            using (SqlConnection conn = new(builder.ConnectionString))
            {
                conn.Open();
                int clientSPID = conn.ServerProcessId;
                int serverSPID = 0;
                InternalConnectionWrapper wrapper = new(conn, true, builder.ConnectionString);
                using SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT @@SPID";
                using (SqlDataReader reader = cmd.ExecuteReader())
                    while (reader.Read())
                    {
                        serverSPID = reader.GetInt16(0);
                    }

                Assert.Equal(serverSPID, clientSPID);
                // Also check SPID after query execution
                Assert.Equal(serverSPID, conn.ServerProcessId);

                wrapper.KillConnectionByTSql();

                // Connection resiliency should reconnect transparently
                using (SqlDataReader reader = cmd.ExecuteReader())
                    while (reader.Read())
                    {
                        serverSPID = reader.GetInt16(0);
                    }

                // SPID should match server's SPID
                Assert.Equal(serverSPID, conn.ServerProcessId);
            }
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsTCPConnectionStringPasswordIncluded))]
        public static void ConnectionStringPersistentInfoTest()
        {
            SqlConnectionStringBuilder connectionStringBuilder = new(DataTestUtility.TCPConnectionString)
            {
                PersistSecurityInfo = false
            };
            string cnnString = connectionStringBuilder.ConnectionString;

            connectionStringBuilder.Clear();
            using (SqlConnection sqlCnn = new(cnnString))
            {
                sqlCnn.Open();
                connectionStringBuilder.ConnectionString = sqlCnn.ConnectionString;
                Assert.True(string.IsNullOrEmpty(connectionStringBuilder.Password), "Password must not persist according to set the PersistSecurityInfo by false!");
            }

            connectionStringBuilder.ConnectionString = DataTestUtility.TCPConnectionString;
            connectionStringBuilder.PersistSecurityInfo = true;
            cnnString = connectionStringBuilder.ConnectionString;

            connectionStringBuilder.Clear();
            using (SqlConnection sqlCnn = new(cnnString))
            {
                sqlCnn.Open();
                connectionStringBuilder.ConnectionString = sqlCnn.ConnectionString;
                Assert.True(!string.IsNullOrEmpty(connectionStringBuilder.Password), "Password must persist according to set the PersistSecurityInfo by true!");
            }
        }

        // ConnectionOpenDisableRetry relies on error 4060 for automatic retry, which is not returned when using AAD auth
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureServer), nameof(DataTestUtility.TcpConnectionStringDoesNotUseAadAuth))]
        public static void ConnectionOpenDisableRetry()
        {
            SqlConnectionStringBuilder connectionStringBuilder = new(DataTestUtility.TCPConnectionString)
            {
                InitialCatalog = "DoesNotExist0982532435423",
                Pooling = false,
                ConnectTimeout = 15,
                ConnectRetryCount = 3
            };
            using SqlConnection sqlConnection = new(connectionStringBuilder.ConnectionString);
            Stopwatch timer = new();

            timer.Start();
            Assert.Throws<SqlException>(() => sqlConnection.Open(SqlConnectionOverrides.OpenWithoutRetry));
            timer.Stop();
            TimeSpan duration = timer.Elapsed;
            Assert.True(duration.Seconds < 2, $"Connection Open() without retries took longer than expected. Expected < 2 sec. Took {duration.Seconds} sec.");

            timer.Restart();
            Assert.Throws<SqlException>(() => sqlConnection.Open());
            timer.Stop();
            duration = timer.Elapsed;
            Assert.True(duration.Seconds > 5, $"Connection Open() with retries took less time than expected. Expect > 5 sec with transient fault handling. Took {duration.Seconds} sec.");                  //    sqlConnection.Open();
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureServer), nameof(DataTestUtility.TcpConnectionStringDoesNotUseAadAuth))]
        public static async Task ConnectionOpenAsyncDisableRetry()
        {
            SqlConnectionStringBuilder connectionStringBuilder = new(DataTestUtility.TCPConnectionString)
            {
                InitialCatalog = DataTestUtility.GetUniqueNameForSqlServer("DoesNotExist", false),
                Pooling = false,
                ConnectTimeout = 15,
                ConnectRetryCount = 3
            };
            using SqlConnection sqlConnection = new(connectionStringBuilder.ConnectionString);
            Stopwatch timer = new();

            timer.Start();
            await Assert.ThrowsAsync<SqlException>(async () => await sqlConnection.OpenAsync(SqlConnectionOverrides.OpenWithoutRetry, CancellationToken.None));
            timer.Stop();
            TimeSpan duration = timer.Elapsed;
            Assert.True(duration.Seconds < 2, $"Connection OpenAsync() without retries took longer than expected. Expected < 2 sec. Took {duration.Seconds} sec.");

            timer.Restart();
            await Assert.ThrowsAsync<SqlException>(async () => await sqlConnection.OpenAsync(CancellationToken.None));
            timer.Stop();
            duration = timer.Elapsed;
            Assert.True(duration.Seconds > 5, $"Connection OpenAsync() with retries took less time than expected. Expect > 5 sec with transient fault handling. Took {duration.Seconds} sec.");
        }

        [PlatformSpecific(TestPlatforms.Windows)]
        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsSQLAliasSetup))]
        public static void ConnectionAliasTest()
        {
            SqlConnectionStringBuilder builder = new(DataTestUtility.TCPConnectionString)
            {
                DataSource = DataTestUtility.AliasName
            };
            using SqlConnection sqlConnection = new(builder.ConnectionString);
            Assert.Equal(DataTestUtility.AliasName, builder.DataSource);
            try
            {
                sqlConnection.Open();
                Assert.Equal(ConnectionState.Open, sqlConnection.State);
            }
            catch (SqlException ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        private static bool CanUseDacConnection()
        {
            if (!DataTestUtility.IsTCPConnStringSetup())
            {
                return false;
            }

            SqlConnectionStringBuilder b = new(DataTestUtility.TCPConnectionString);
            if (!DataTestUtility.ParseDataSource(b.DataSource, out string hostname, out int port, out string instanceName))
            {
                return false;
            }

            if ("localhost".Equals(hostname.ToLower()) && (port.Equals(-1) || port.Equals(1433)) &&
                string.IsNullOrEmpty(instanceName) && b.UserID != null && b.UserID.ToLower().Equals("sa"))
            {
                return true;
            }

            return false;
        }

        [ConditionalFact(nameof(CanUseDacConnection))]
        public static void DacConnectionTest()
        {
            if (!CanUseDacConnection())
            {
                throw new Exception("Unable to use a DAC connection in this environment. Localhost + sa credentials required.");
            }

            SqlConnectionStringBuilder b = new(DataTestUtility.TCPConnectionString);
            b.DataSource = "admin:localhost";
            using SqlConnection sqlConnection = new(b.ConnectionString);
            sqlConnection.Open();
        }

        private static bool UsernamePasswordNonEncryptedConnectionSetup()
        {
            if (!DataTestUtility.IsTCPConnStringSetup())
            {
                return false;
            }

            SqlConnectionStringBuilder b = new(DataTestUtility.TCPConnectionString);
            return !string.IsNullOrEmpty(b.UserID) &&
                !string.IsNullOrEmpty(b.Password) &&
                b.Encrypt == SqlConnectionEncryptOption.Optional &&
                (b.Authentication == SqlAuthenticationMethod.NotSpecified || b.Authentication == SqlAuthenticationMethod.SqlPassword);
        }

        [ConditionalFact(nameof(UsernamePasswordNonEncryptedConnectionSetup))]
        public static void SqlPasswordConnectionTest()
        {
            if (!UsernamePasswordNonEncryptedConnectionSetup())
            {
                throw new Exception("Sql credentials and non-Encrypted connection required.");
            }

            SqlConnectionStringBuilder b = new(DataTestUtility.TCPConnectionString);
            b.Authentication = SqlAuthenticationMethod.SqlPassword;

            // This ensures we are not validating the server certificate when we shouldn't be
            // This test may fail if Encrypt = false but the test server requires encryption
            b.TrustServerCertificate = false;

            using SqlConnection sqlConnection = new(b.ConnectionString);
            sqlConnection.Open();
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.AreConnStringsSetup), nameof(DataTestUtility.IsNotAzureSynapse))]
        public static void ConnectionFireInfoMessageEventOnUserErrorsShouldSucceed()
        {
            using (var connection = new SqlConnection(DataTestUtility.TCPConnectionString))
            {
                string command = "print";
                string commandParam = "OK";

                connection.FireInfoMessageEventOnUserErrors = true;

                connection.InfoMessage += (sender, args) =>
                {
                    Assert.Equal(commandParam, args.Message);
                };

                connection.Open();

                using SqlCommand cmd = connection.CreateCommand();
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = $"{command} '{commandParam}'";
                cmd.ExecuteNonQuery();
            }
        }
    }
}
