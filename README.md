# ParquetToSql

ParquetToSql is a super simple and lightweight IDataReader that will read over the data in a parquet file. It uses the fabulous [Parquet.Net](https://github.com/elastacloud/parquet-dotnet) library to do all the hard work.

It was written specifically to bulk load a parquet file into a sql table in cases where the parquet file schema needs to be dynamic. If you have tables to load with a set schema, consider making a poco and using [Parquet.Net](https://github.com/elastacloud/parquet-dotnet) to hydrate an IEnumerable of that poco, then use [FastMember](https://github.com/mgravell/fast-member) to get an IDataReader for bulk loading.

# Usage

Assuming the parquet file and the sql table match perfectly (both in name and data type) then this should do the trick. For information on data type conversion or column mappings, see the documentation for the SqlBulkCopy class.

```C#
using (var parquetReader = new ParquetReader(fileStream))
{
  using(var parquetDataReader = new ParquetDataReader(parquetReader))
  {
    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
    {
      bulkCopy.DestinationTableName = "dbo.BulkCopyDemoMatchingColumns";
      bulkCopy.WriteToServer(parquetDataReader);
    }
  }
}
```
