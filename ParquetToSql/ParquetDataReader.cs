using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ParquetToSql
{
    /// <summary>
    /// This is intended to make it easy to bulk load a parquet file using the SqlBulkCopy api
    /// </summary>
    public class ParquetDataReader : IDataReader
    {
        private readonly ParquetReader _parquetReader;
        private readonly Lazy<List<Func<object, object>>> _columnConverters;
        private readonly int _rowGroupCount;
        private int _rowGroupIndex;
        private long _rowIndex;
        private long _rowCount;

        private bool _opened = false;
        private bool _closed = false;

        private List<DataField> _fields;
        private List<Parquet.Data.DataColumn> _columns;

        public ParquetDataReader(ParquetReader parquetReader, IEnumerable<DataField> fields, IEnumerable<ColumnConverter> columnConverters)
        {
            _parquetReader = parquetReader ?? throw new ArgumentNullException(nameof(parquetReader));
            _fields = fields?.ToList() ?? throw new ArgumentNullException(nameof(fields));
            _rowGroupCount = parquetReader.RowGroupCount;
            _columnConverters = new Lazy<List<Func<object, object>>>(() => InitializeColumnConverters(columnConverters), true);
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

        public ParquetDataReader(ParquetReader parquetReader) : this(parquetReader, parquetReader?.Schema?.GetDataFields(), null) { }

        public ParquetDataReader(ParquetReader parquetReader, IEnumerable<ColumnConverter> columnConverters) : this(parquetReader, parquetReader?.Schema?.GetDataFields(), columnConverters) { }

        object IDataRecord.this[int i] => Get(i);

        object IDataRecord.this[string name] => Get(GetOrdinal(name));

        int IDataReader.Depth => 0;

        bool IDataReader.IsClosed => _closed;

        int IDataReader.RecordsAffected => 0;

        int IDataRecord.FieldCount => _fields.Count;

        void IDataReader.Close()
        {
            _closed = true;
            _columns = null;
        }

        bool IDataRecord.GetBoolean(int i) => (bool) Get(i);

        byte IDataRecord.GetByte(int i) => (byte)Get(i);

        long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetBytes));
        }

        long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetChars));
        }

        char IDataRecord.GetChar(int i) => (char)Get(i);

        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetData));
        }

        string IDataRecord.GetDataTypeName(int i)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetDataTypeName));
        }

        DateTime IDataRecord.GetDateTime(int i)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetDateTime));
        }

        decimal IDataRecord.GetDecimal(int i) => (decimal)Get(i);

        double IDataRecord.GetDouble(int i) => (double)Get(i);

        System.Type IDataRecord.GetFieldType(int i)
        {
            throw new NotImplementedException(nameof(IDataRecord.GetFieldType));
        }

        float IDataRecord.GetFloat(int i) => (float)Get(i);

        Guid IDataRecord.GetGuid(int i) => (Guid)Get(i);

        short IDataRecord.GetInt16(int i) => (short)Get(i);

        int IDataRecord.GetInt32(int i) => (int)Get(i);

        long IDataRecord.GetInt64(int i) => (long)Get(i);

        string IDataRecord.GetName(int i) => _fields[i]?.Name;

        int IDataRecord.GetOrdinal(string name) => GetOrdinal(name);

        DataTable IDataReader.GetSchemaTable()
        {
            throw new NotImplementedException(nameof(IDataReader.GetSchemaTable));
        }

        string IDataRecord.GetString(int i) => (string)Get(i);

        object IDataRecord.GetValue(int i) => Get(i);

        int IDataRecord.GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        bool IDataRecord.IsDBNull(int i) => Get(i) == null;

        bool IDataReader.NextResult()
        {
            _closed = true;
            return false;
        }

        bool IDataReader.Read()
        {
            if (_closed)
                return false;

            // first
            if (!_opened)
            {
                if (_rowGroupCount > 0)
                {
                    _opened = true;
                    return LoadRowGroup(_rowGroupIndex);
                }
                else
                {
                    _closed = true;
                    return false;
                }
            }
            else
            {
                _rowIndex++;
                if (_rowIndex < _rowCount)
                {
                    return true;
                }
                else
                {
                    _rowGroupIndex++;
                    if (_rowGroupIndex < _rowGroupCount)
                    {
                        return LoadRowGroup(_rowGroupIndex);
                    }
                    else
                    {
                        _closed = true;
                        _columns = null;
                        return false;
                    }
                }
            }
        }

        private bool LoadRowGroup(int rowGroup)
        {
            using (var reader = _parquetReader.OpenRowGroupReader(rowGroup))
            {
                _rowIndex = 0;
                _rowCount = reader.RowCount;

                var columns = new List<Parquet.Data.DataColumn>();
                foreach (var f in _fields)
                {
                    try
                    {
                        columns.Add(reader.ReadColumn(f));
                    }
                    catch (IndexOutOfRangeException)
                    {
                        // this happens if every single element in the column within the rowgroup is null,
                        // TODO: Figure out how to read the header to detect this without an exception
                        columns.Add(null);
                    }
                }
                _columns = columns;
            }
            return true;
        }

        private object Get(int i)
        {
            if (_opened && !_closed)
            {
                var column = _columns[i];
                var converter = _columnConverters.Value[i];

                return converter(new Func<object>(() =>
                {
                    if (column == null)
                        return null;
                    else
                        return column.Data.GetValue(_rowIndex);
                })());
            }
            else
            {
                throw new ApplicationException("Invalid state for getting data");
            }
        }

        private int GetOrdinal(string name)
        {
            return _fields.FindIndex(_ => string.Equals(_.Name, name, StringComparison.InvariantCultureIgnoreCase));
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
                _columns = null; // potentially silly, but part of the dispose pattern.

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion

    }
}
