using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetToSql
{
    public class ParquetDataWriter : IDisposable
    {
        private readonly IDataReader _dataReader;
        private readonly IEnumerable<DataField> _fields;
        private readonly int _rowGroupSize;
        private readonly Lazy<List<Func<object, object>>> _columnConverters;

        public ParquetDataWriter(IDataReader dataReader, IEnumerable<DataField> fields, IEnumerable<ColumnConverter> columnConverters, int rowgroupSize)
        {
            _dataReader = dataReader ?? throw new ArgumentNullException(nameof(dataReader)); ;
            _fields = fields ?? throw new ArgumentNullException(nameof(fields));
            _rowGroupSize = rowgroupSize;
            _columnConverters = new Lazy<List<Func<object, object>>>(() => InitializeColumnConverters(columnConverters), true);
        }
        public ParquetDataWriter(IDataReader dataReader, IEnumerable<ColumnConverter> columnConverters, int rowgroupSize = 50000)
        {
        }

        private List<Func<object, object>> InitializeColumnConverters(IEnumerable<ColumnConverter> columnConverters)
        {
            var list = new List<Func<object, object>>();
            foreach (var field in _fields)
            {
                var converter = columnConverters?.FirstOrDefault(_ => field.Name != null && _.ColumnName == field.Name);
                if (converter?.Converter != null)
                {
                    list.Add(converter.Converter);
                }
                else
                {
                    list.Add(_ => _);
                }
            }
            return list;
        }

        public long Write(string filename)
        {
            long rows = 0;
            using (var fs = new FileStream(filename, FileMode.Create))
            {
                rows = Write(fs);
            }
            // remove empty invalid file
            if (rows == 0 && File.Exists(filename))
                File.Delete(filename);
            return rows;
        }

        public long Write(Stream stream)
        {
            long totalRowCount = 0;
            var schema = new Schema(_fields.ToList().AsReadOnly());
            using (var writer = new ParquetWriter(schema, stream))
            {
                int rowIndex = 0;
                var data = new Dictionary<DataField, List<object>>();
                while (_dataReader.Read())
                {
                    totalRowCount++;
                    int fIndex = 0;
                    foreach (var f in _fields)
                    {
                        var value = _dataReader[f.Name];
                        value = _columnConverters.Value[fIndex](value);
                        fIndex++;
                        if (!data.ContainsKey(f))
                            data.Add(f, new List<object>());
                        data[f].Add(value);
                    }
                    rowIndex++;
                    if (!(rowIndex < _rowGroupSize))
                    {
                        FlushRowGroup(writer, data);
                        rowIndex = 0;
                    }
                }

                FlushRowGroup(writer, data);
            }
            return totalRowCount;
        }

        private void FlushRowGroup(ParquetWriter writer, Dictionary<DataField, List<object>> data)
        {
            if (data.Count > 0 && data[data.Keys.First()].Count > 0)
            {
                using (var rowGroup = writer.CreateRowGroup())
                {
                    foreach (var f in _fields)
                    {
                        var column = new Parquet.Data.DataColumn(f, GetColumnArray(data, f));
                        rowGroup.WriteColumn(column);
                    }
                }
                data.Clear();
            }
        }

        private static Array GetColumnArray(Dictionary<DataField, List<object>> data, DataField f)
        {
            switch (f.DataType)
            {
                // hate this, but parquet.net needs the boxed arrays to be the correct type
                case DataType.Boolean:
                    return GetColumnArray<bool>(data, f);
                case DataType.Byte:
                    return GetColumnArray<byte>(data, f);
                case DataType.ByteArray:
                    return GetColumnArray<byte[]>(data, f);
                case DataType.DateTimeOffset:
                    return GetColumnArray<DateTimeOffset>(data, f);
                case DataType.Decimal:
                    return GetColumnArray<decimal>(data, f);
                case DataType.Double:
                    return GetColumnArray<double>(data, f);
                case DataType.Float:
                    return GetColumnArray<float>(data, f);
                case DataType.Int16:
                    return GetColumnArray<short>(data, f);
                case DataType.Int32:
                    return GetColumnArray<int>(data, f);
                case DataType.Int64:
                    return GetColumnArray<long>(data, f);
                case DataType.Interval:
                    return GetColumnArray<TimeSpan>(data, f);
                case DataType.String:
                    return GetColumnArray<string>(data, f);
                default:
                    return GetColumnArray<object>(data, f);
            }
        }

        private static T[] GetColumnArray<T>(Dictionary<DataField, List<object>> data, DataField f)
        {
            return data[f].Cast<T>().ToArray();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~ParquetDataWriter()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}