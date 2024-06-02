using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
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
    }
}
