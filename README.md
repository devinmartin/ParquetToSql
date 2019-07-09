# ParquetToSql

ParquetToSql is a super simple and lightweight IDataReader that will read over the data in a parquet file. It uses the fabulous [Parquet.Net](https://github.com/elastacloud/parquet-dotnet) library to do all the hard work.

It was written specifically to bulk load a parquet file into a sql table. It handles cases where the parquet file schema isn't known at compile time.

# Basic Usage
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

# Subset of Columns

The great thing about a columnar format like parquet is that you can select just a few columns. This will result in less data read and a faster overall operation. Simply pass a collection of DataFields from parquet.net into the ParquetDataReader. Only the items in the collection will be read.

```C#
using (var parquetDataReader = new ParquetDataReader(parquetReader, new DataField[] { new DataField(...) }))
{
...
```
