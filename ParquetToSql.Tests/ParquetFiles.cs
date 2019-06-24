using Parquet;
using Parquet.Data;
using Parquet.Serialization;
using ParquetToSql.Tests.ParquetSamples;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ParquetToSql.Tests
{
    static class ParquetFiles
    {
        public static Stream GetParquetFileWithThreeRowGroups()
        {
            var stream = new MemoryStream();
            var schema = SchemaReflector.Reflect<TwoColumn>();
            using (var parquetWriter = new ParquetWriter(schema, stream))
            {
                using (var rowGroup = parquetWriter.CreateRowGroup())
                {
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[0], new[] {
                        1,
                        2,
                        3,
                        4
                        }));
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[1], new[] {
                        "one",
                        "two",
                        "three",
                        "four"
                        }));
                }

                using (var rowGroup = parquetWriter.CreateRowGroup())
                {
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[0], new[] {
                        5,
                        6,
                        7,
                        8
                        }));
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[1], new[] {
                        "five",
                        "six",
                        "seven",
                        "eight"
                        }));
                }

                using (var rowGroup = parquetWriter.CreateRowGroup())
                {
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[0], new[] {
                        9,
                        10,
                        11,
                        12
                        }));
                    rowGroup.WriteColumn(new Parquet.Data.DataColumn((DataField)schema.Fields[1], new[] {
                        "nine",
                        "ten",
                        "eleven",
                        "twelve"
                        }));
                }
            }

            stream.Position = 0;
            return stream;
        }
    }
}
