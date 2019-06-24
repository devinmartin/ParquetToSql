using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Parquet;
using ParquetToSql.Tests.ParquetSamples;
using System;
using System.Data;

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
        public void GetColumnData()
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


        private void ReadTest(Action<IDataReader> test)
        {
            using (var stream = ParquetFiles.GetParquetFileWithThreeRowGroups())
            {
                using (var parquetReader = new ParquetReader(stream))
                {
                    using (IDataReader reader = new ParquetDataReader(parquetReader))
                    {
                        test(reader);
                    }
                }
            }
        }
    }
}
