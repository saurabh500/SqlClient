using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.SqlClientX;
using Xunit;
using Xunit.Abstractions;

namespace TestApplication
{
    public  class SqlXmlTest
    {
        private readonly ITestOutputHelper _output;

        public SqlXmlTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void TestXmlRetrieval()
        {

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "tcp:localhost,1444";
            builder.IntegratedSecurity = true;
            builder.InitialCatalog = "testdatabase";
            builder.TrustServerCertificate = true;
            builder.Encrypt = false;

            string query = "select [EmployeeDetails] from dbo.Employees";

            using SqlConnection connection = new SqlConnection(builder.ConnectionString);
            connection.Open();
            using SqlCommand command = connection.CreateCommand();
            command.CommandText = query;
            using SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                _output.WriteLine(reader.GetSqlXml(0).Value);
            }
        }

        [Fact]
        public async void TestConnection()
        {
            string connectionString = $"Server=tcp:192.168.1.83,1444;" +
                        $"Min Pool Size=0;Max Pool Size = 200;User Id=sa; pwd=HappyPass1234; " +
                        $"Connection Timeout=30;TrustServerCertificate=True;Timeout=0;Encrypt=False;Database=master;Pooling=True;" +
                        "Application Name=TestAppX; MultipleActiveResultSets=True"; // pooled
            SqlConnectionX connection = new SqlConnectionX(connectionString);
            await connection.OpenAsync();
        }
    }
}
