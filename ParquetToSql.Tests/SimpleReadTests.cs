using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using Parquet.Data;
using ParquetToSql.Tests.ParquetSamples;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ParquetToSql.Tests
{
    [TestClass]
    public class SimpleReadTests
    {
        [TestMethod]
        public void CanReadAllRows()
        {
            // arrange
            int rowCount = 0;

            // act
            ReadTest(reader =>
            {
                while (reader.Read())
                {
                    rowCount++;
                }
            });

            // assert
            rowCount.Should().Be(12);
        }

        [TestMethod]
        public void GetColumnMetaData()
        {
            // arrange
            string column1Name = null;
            string column2Name = null;

            // act
            ReadTest(reader =>
            {
                column1Name = reader.GetName(0);
                column2Name = reader.GetName(1);
            });

            // assert
            column1Name.Should().Be(nameof(TwoColumn.Column1));
            column2Name.Should().Be(nameof(TwoColumn.Column2));
        }

        [TestMethod]
        public void GetColumnData()
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

        [TestMethod]
        public void ConverterTest()
        {
            // arrange
            var column1Data = new List<int>();
            var column2Data = new List<int>();

            // create a converter for Column2 by name that converts a string to int (length)
            var converters = new[] { new ColumnConverter { ColumnName = nameof(TwoColumn.Column2), Converter = _ => ((string)_).Length } };

            // act
            ReadDataTest(reader =>
            {
                column1Data.Add(reader.GetInt32(0));

                // even though the file contains string, our converter gives us len as an int
                column2Data.Add(reader.GetInt32(1));
            }, converters);

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

            column2Data.Should().Equal((new[] {
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
                        }).Select(_ => _.Length));
        }

        [TestMethod]
        public void ReadSingleColumnData()
        {
            // arrange
            var column2Data = new List<string>();
            int numberOfColumns = 0;
            // act
            ReadTest(reader =>
            {
                
                while (reader.Read())
                {
                    numberOfColumns = reader.FieldCount;
                    column2Data.Add(reader.GetString(0));
                }
            }, new DataField(nameof(TwoColumn.Column2), typeof(string)));

            // assert
            numberOfColumns.Should().Be(1);
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
            using (var stream = ParquetFiles.GetParquetFileWithThreeRowGroups())
            {
                using (var parquetReader = new ParquetReader(stream))
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
