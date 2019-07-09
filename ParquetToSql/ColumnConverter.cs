using System;

namespace ParquetToSql
{
    public class ColumnConverter
    {
        public string ColumnName { get; set; }
        public Func<object, object> Converter { get; set; }
    }
}