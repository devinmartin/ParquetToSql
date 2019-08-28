using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using Parquet.Data;
using ParquetToSql.Tests.ParquetSamples;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace ParquetToSql.Tests
{
    [TestClass]
    public class SimpleWriteTests
    {
        [TestMethod]
        public void WriteColumnData()
        {
            // arrange
            var column1Data = new List<int>();
            var column2Data = new List<string>();

            // act
            ReadDataTest(reader =>
            {
                column1Data.Add(reader.GetInt32(0));
                column2Data.Add(reader.GetString(1));
            });

            // assert
            column1Data.Should().Equal(new[] {
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12
                        });

            column2Data.Should().Equal(new[] {
                        "one",
                        "two",
                        "three",
                        "four",
                        "five",
                        "six",
                        "seven",
                        "eight",
                        "nine",
                        "ten",
                        "eleven",
                        "twelve"
                        });
        }


        private void ReadTest(Action<IDataReader> test, params DataField[] dataFields)
        {
            using (var stream = ParquetFiles.GetParquetFileWithThreeRowGroups())
            {
                using (var parquetReader = new ParquetReader(stream))
                {
                    using (IDataReader reader = (dataFields?.Length > 0) ? new ParquetDataReader(parquetReader, dataFields, null) : new ParquetDataReader(parquetReader))
                    {
                        test(reader);
                    }
                }
            }
        }
        private void ReadDataTest(Action<IDataReader> test, IEnumerable<ColumnConverter> columnConverters = null)
        {
            using (var intermediate = new MemoryStream())
            {
                using (var stream = ParquetFiles.GetParquetFileWithThreeRowGroups())
                {
                    using (var parquetReader = new ParquetReader(stream))
                    {
                        using (IDataReader reader = new ParquetDataReader(parquetReader, columnConverters))
                        {
                            using (var writer = new ParquetDataWriter(reader, parquetReader.Schema.GetDataFields(), columnConverters, 2))
                            {
                                writer.Write(intermediate);
                            }
                        }
                    }
                }
                intermediate.Position = 0;
                using (var parquetReader = new ParquetReader(intermediate))
                {
                    using (IDataReader reader = new ParquetDataReader(parquetReader, columnConverters))
                    {
                        while (reader.Read())
                        {
                            test(reader);
                        }
                    }
                }
            }
        }
    }
}
