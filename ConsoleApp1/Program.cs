using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Text;
class SqlBulkCopyDemo
{
    private static string connectionString = Environment.GetEnvironmentVariable("CONNECTION_STR") ?? "Server=tcp:10.224.90.151;Database=Demo2;Encrypt=False; User ID=sa; PWD=Yukon900!Welcome";

    static void Main()
    {
        MainDataReadStream();
        Debug.WriteLine("-----------------");
        Console.WriteLine("-----------------");
        //MainBulkCopy();
    }

    static void MainDataReadStream()
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            Console.OutputEncoding = Encoding.UTF8;
            connection.Open();
            string _outputFile = "serverRecords.json";

            using (SqlCommand command = new SqlCommand("SELECT [data] FROM [jsonTab]", connection))
            {
                using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
                {

                    while (reader.Read())
                    {

                        char[] buff = new char[4096];
                        using (TextReader data = reader.GetTextReader(0))
                        {

                            int readCount = 0;

                            do
                            {
                                readCount = data.ReadBlock(buff, 0, 4096);
                            } while (readCount > 0);
                            //Console.WriteLine(data.ReadToEnd());

                        }
                    }
                }
            }
        }
    }

    static void MainBulkCopy()
    {
        // Open a sourceConnection to the AdventureWorks database.
        using (SqlConnection sourceConnection =
                   new SqlConnection(connectionString))
        {
            sourceConnection.Open();
            // Perform an initial count on the destination table.
            SqlCommand commandRowCount = new SqlCommand("SELECT COUNT(*) FROM " + "dbo.jsonTabCopy;", sourceConnection);
            //SqlCommand commandRowCount = new SqlCommand("SELECT COUNT(*) FROM " + "dbo.xmlTabCopy;", sourceConnection);
            //SqlCommand commandRowCount = new SqlCommand("SELECT COUNT(*) FROM " + "dbo.vcmTabCopy;",  sourceConnection);
            long countStart = System.Convert.ToInt32(
                commandRowCount.ExecuteScalar());
            Console.WriteLine("Starting row count = {0}", countStart);
            // Get data from the source table as a SqlDataReader.
            SqlCommand commandSourceData = new SqlCommand("SELECT data FROM dbo.jsonTab;", sourceConnection);
            //SqlCommand commandSourceData = new SqlCommand("SELECT data FROM dbo.xmlTab;", sourceConnection);
            //SqlCommand commandSourceData = new SqlCommand("SELECT data FROM dbo.vcmTab;", sourceConnection);
            SqlDataReader reader = commandSourceData.ExecuteReader(CommandBehavior.SequentialAccess);
            /*while (reader.Read())
            {
                object value = reader.GetValue(0);
                Console.WriteLine("UTF8 Byte Count: "+Encoding.UTF8.GetByteCount((string)value));
            }*/

            // Open the destination connection. In the real world you would 
            // not use SqlBulkCopy to move data from one table to the other 
            // in the same database. This is for demonstration purposes only.
            using (SqlConnection destinationConnection =
                       new SqlConnection(connectionString))
            {
                destinationConnection.Open();
                // Set up the bulk copy object. 
                // Note that the column positions in the source
                // data reader match the column positions in 
                // the destination table so there is no need to
                // map columns.
                using (SqlBulkCopy bulkCopy =
                           new SqlBulkCopy(destinationConnection))
                {
                    bulkCopy.EnableStreaming = true;
                    bulkCopy.DestinationTableName = "dbo.jsonTabCopy";
                    //bulkCopy.DestinationTableName = "dbo.xmlTabCopy";
                    //bulkCopy.DestinationTableName = "dbo.vcmTabCopy";
                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(reader);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                    }
                    finally
                    {
                        // Close the SqlDataReader. The SqlBulkCopy
                        // object is automatically closed at the end
                        // of the using block.
                        reader.Close();
                    }
                }
                // Perform a final count on the destination 
                // table to see how many rows were added.
                long countEnd = System.Convert.ToInt32(
                    commandRowCount.ExecuteScalar());
                Console.WriteLine("Ending row count = {0}", countEnd);
                Console.WriteLine("{0} rows were added.", countEnd - countStart);
                Console.WriteLine("Press Enter to finish.");
                Console.ReadLine();
            }
        }
    }

}
