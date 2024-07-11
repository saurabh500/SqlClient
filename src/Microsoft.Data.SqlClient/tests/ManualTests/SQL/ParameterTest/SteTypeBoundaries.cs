// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Text;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests
{
    public abstract class SteTypeBoundaries : StePermutationGenerator
    {
        // Use this marker for attribute value to indicate the attribute is not used 
        // (ex. option where Decimal parameter's Scale property should not be set at all)
        public static object s_doNotUseMarker = new();
    }

    // simple types can just wrap a simple permutation generator
    public class SteSimpleTypeBoundaries : SteTypeBoundaries
    {
        private static readonly byte[] s_theBigByteArray = CreateByteArray(1000000);
        private static readonly byte[] s_moderateSizeByteArray = CreateByteArray(8000);
        private static readonly string s_moderateSizeString = CreateString(8000);
        private static readonly char[] s_moderateSizeCharArray = s_moderateSizeString.ToCharArray();

        // Class members
        public static readonly IList<SteSimpleTypeBoundaries> s_allTypes;
        public static readonly IList<SteSimpleTypeBoundaries> s_allTypesExceptUdts;
        public static readonly IList<SteSimpleTypeBoundaries> s_udtsOnly;
        static SteSimpleTypeBoundaries()
        {
            List<SteSimpleTypeBoundaries> list = new();

            // DevNote: Don't put null value attributes first -- it confuses DataTable generation for SteStructuredTypeBoundaries

            // BigInt
            SteSimplePermutationGenerator type = new()
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.BigInt },
                { SteAttributeKey.Value, (long)0 },
                { SteAttributeKey.Value, long.MaxValue },
                { SteAttributeKey.Value, long.MinValue },
                { SteAttributeKey.Value, new SqlInt64(long.MaxValue) },
                { SteAttributeKey.Value, new SqlInt64(long.MinValue) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Binary types
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Binary },
                { SteAttributeKey.SqlDbType, SqlDbType2.VarBinary },
                { SteAttributeKey.SqlDbType, SqlDbType2.Image },
                { SteAttributeKey.MaxLength, 1 },    // a small value
                { SteAttributeKey.MaxLength, 40 },   // Somewhere in the middle
                { SteAttributeKey.MaxLength, 8000 }, // Couple values around maximum tds length
                { SteAttributeKey.Value, CreateByteArray(0) },
                { SteAttributeKey.Value, CreateByteArray(1) },
                { SteAttributeKey.Value, CreateByteArray(50) },
                { SteAttributeKey.Value, s_moderateSizeByteArray },
                { SteAttributeKey.Value, new SqlBytes(CreateByteArray(0)) },
                { SteAttributeKey.Value, new SqlBytes(CreateByteArray(1)) },
                { SteAttributeKey.Value, new SqlBytes(CreateByteArray(40)) },
                { SteAttributeKey.Value, new SqlBytes(s_moderateSizeByteArray) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value },
                { SteAttributeKey.Offset, s_doNotUseMarker },
                { SteAttributeKey.Offset, -1 },
                { SteAttributeKey.Offset, 0 },
                { SteAttributeKey.Offset, 10 },
                { SteAttributeKey.Offset, 8000 },
                { SteAttributeKey.Offset, int.MaxValue },
                { SteAttributeKey.Length, 0 },
                { SteAttributeKey.Length, 40 },
                { SteAttributeKey.Length, 8000 },
                { SteAttributeKey.Length, 1000000 },
                { SteAttributeKey.Length, -1 }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Byte
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.TinyInt },
                { SteAttributeKey.Value, byte.MaxValue },
                { SteAttributeKey.Value, byte.MinValue },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Character (ANSI)
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Char },
                { SteAttributeKey.SqlDbType, SqlDbType2.Text },
                { SteAttributeKey.SqlDbType, SqlDbType2.VarChar },
                { SteAttributeKey.MaxLength, 1 },
                { SteAttributeKey.MaxLength, 30 },
                { SteAttributeKey.MaxLength, 8000 },
                { SteAttributeKey.Value, CreateString(1) },
                { SteAttributeKey.Value, CreateString(20) },
                { SteAttributeKey.Value, s_moderateSizeString },
                { SteAttributeKey.Value, CreateString(1).ToCharArray() },
                { SteAttributeKey.Value, CreateString(25).ToCharArray() },
                { SteAttributeKey.Value, s_moderateSizeCharArray },
                { SteAttributeKey.Value, new SqlChars(CreateString(1).ToCharArray()) },
                { SteAttributeKey.Value, new SqlChars(CreateString(30).ToCharArray()) },
                { SteAttributeKey.Value, new SqlChars(s_moderateSizeCharArray) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Character (UNICODE)
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.NChar },
                { SteAttributeKey.SqlDbType, SqlDbType2.NText },
                { SteAttributeKey.SqlDbType, SqlDbType2.NVarChar },
                { SteAttributeKey.MaxLength, 1 },
                { SteAttributeKey.MaxLength, 35 },
                { SteAttributeKey.MaxLength, 4000 },
                { SteAttributeKey.Value, CreateString(1) },
                { SteAttributeKey.Value, CreateString(15) },
                { SteAttributeKey.Value, s_moderateSizeString },
                { SteAttributeKey.Value, CreateString(1).ToCharArray() },
                { SteAttributeKey.Value, CreateString(20).ToCharArray() },
                { SteAttributeKey.Value, s_moderateSizeCharArray },
                { SteAttributeKey.Value, new SqlChars(CreateString(1).ToCharArray()) },
                { SteAttributeKey.Value, new SqlChars(CreateString(25).ToCharArray()) },
                { SteAttributeKey.Value, new SqlChars(s_moderateSizeCharArray) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // DateTime
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.DateTime },
                { SteAttributeKey.SqlDbType, SqlDbType2.SmallDateTime },
                { SteAttributeKey.Value, new DateTime(1753, 1, 1) },
                { SteAttributeKey.Value, new SqlDateTime(new DateTime(1753, 1, 1)) },  // min SqlDateTime
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Decimal
            //  the TVP test isn't robust in the face of OverflowExceptions on input, so a number of these
            //  values are commented out and other numbers substituted.
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Decimal },
                { SteAttributeKey.Precision, (byte)38 },
                { SteAttributeKey.Scale, (byte)0 },
                { SteAttributeKey.Scale, (byte)10 },
                { SteAttributeKey.Value, (decimal)0 },
                { SteAttributeKey.Value, decimal.MaxValue / 10000000000 },
                { SteAttributeKey.Value, new SqlDecimal(0) },
                { SteAttributeKey.Value, ((SqlDecimal)1234567890123456.789012345678M) * 100 }, // Bigger than a Decimal
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Float
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Float },
                { SteAttributeKey.Value, (double)0 },
                { SteAttributeKey.Value, double.MaxValue },
                { SteAttributeKey.Value, double.MinValue },
                { SteAttributeKey.Value, new SqlDouble(double.MaxValue) },
                { SteAttributeKey.Value, new SqlDouble(double.MinValue) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Int
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Int },
                { SteAttributeKey.Value, (int)0 },
                { SteAttributeKey.Value, int.MaxValue },
                { SteAttributeKey.Value, int.MinValue },
                { SteAttributeKey.Value, new SqlInt32(int.MaxValue) },
                { SteAttributeKey.Value, new SqlInt32(int.MinValue) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Money types
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Money },
                { SteAttributeKey.SqlDbType, SqlDbType2.SmallMoney },
                { SteAttributeKey.Value, (decimal)0 },
                { SteAttributeKey.Value, (decimal)unchecked(((long)0x8000000000000000L) / 10000) },
                { SteAttributeKey.Value, (decimal)0x7FFFFFFFFFFFFFFFL / 10000 },
                { SteAttributeKey.Value, new decimal(-214748.3648) }, // smallmoney min
                { SteAttributeKey.Value, new decimal(214748.3647) }, // smallmoney max
                { SteAttributeKey.Value, new SqlMoney(((decimal)int.MaxValue) / 10000) },
                { SteAttributeKey.Value, new SqlMoney(((decimal)int.MinValue) / 10000) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // Real
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.Real },
                { SteAttributeKey.Value, (float)0 },
                { SteAttributeKey.Value, float.MaxValue },
                { SteAttributeKey.Value, float.MinValue },
                { SteAttributeKey.Value, new SqlSingle(float.MaxValue) },
                { SteAttributeKey.Value, new SqlSingle(float.MinValue) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // SmallInt
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.SmallInt },
                { SteAttributeKey.Value, (short)0 },
                { SteAttributeKey.Value, short.MaxValue },
                { SteAttributeKey.Value, short.MinValue },
                { SteAttributeKey.Value, new SqlInt16(short.MaxValue) },
                { SteAttributeKey.Value, new SqlInt16(short.MinValue) },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // UniqueIdentifier
            type = new SteSimplePermutationGenerator
            {
                { SteAttributeKey.SqlDbType, SqlDbType2.UniqueIdentifier },
                { SteAttributeKey.Value, new Guid() },
                { SteAttributeKey.Value, null },
                { SteAttributeKey.Value, DBNull.Value }
            };
            list.Add(new SteSimpleTypeBoundaries(type));

            // UDT
            // UDTs aren's supported in all table scenarios, so make a separate list
            // that doesn't include them.
            s_allTypesExceptUdts = new List<SteSimpleTypeBoundaries>(list).AsReadOnly();

            //type = new SteSimplePermutationGenerator();
            //type.Add(SteAttributeKey.SqlDbType, SqlDbType2.Udt);
            //type.Add(SteAttributeKey.TypeName, "dbo.WeakAddress");
            //type.Add(SteAttributeKey.Type, typeof(WeakAddress));
            //type.Add(SteAttributeKey.Value, new WeakAddress("", ""));
            //type.Add(SteAttributeKey.Value, new WeakAddress(CreateString(22), ""));
            //type.Add(SteAttributeKey.Value, new WeakAddress(ModerateSizeString, ""));
            //type.Add(SteAttributeKey.Value, null);
            //type.Add(SteAttributeKey.Value, DBNull.Value);
            //
            //SteSimpleTypeBoundaries udt = new SteSimpleTypeBoundaries(type);
            //list.Add(udt);
            //
            //AllTypes = list.AsReadOnly();
            //
            //list = new List<SteSimpleTypeBoundaries>();
            //list.Add(udt);
            //UdtsOnly = list.AsReadOnly();
        }

        private readonly SteSimplePermutationGenerator _generator;

        public SteSimpleTypeBoundaries(SteSimplePermutationGenerator generator)
        {
            _generator = generator;
        }

        public override IEnumerable<SteAttributeKey> DefaultKeys
        {
            get
            {
                return _generator.DefaultKeys;
            }
        }

        public override IEnumerator<StePermutation> GetEnumerator(IEnumerable<SteAttributeKey> keysOfInterest)
        {
            return _generator.GetEnumerator(keysOfInterest);
        }

        private const string Prefix = "Char: ";
        public static string CreateString(int size)
        {
            StringBuilder b = new();
            b.Append(Prefix);
            for (int i = 0; i < s_theBigByteArray.Length && b.Length < size; i++)
            {
                b.Append(s_theBigByteArray[i]);
            }

            if (b.Length > size)
            {
                b.Remove(size, b.Length - size);
            }

            return b.ToString();
        }

        public static byte[] CreateByteArray(int size)
        {
            byte[] result = new byte[size];

            // 
            // Leave a marker of three 0s, followed by the cycle count
            int cycleCount = 0;
            byte cycleStep = 0;
            for (int i = 0; i < result.Length; i++)
            {
                if (cycleStep < 3)
                {
                    result[i] = 0;
                }
                else if (3 == cycleStep)
                {
                    result[i] = (byte)cycleCount;
                }
                else
                {
                    result[i] = cycleStep;
                }
                if (cycleStep == byte.MaxValue)
                {
                    cycleCount++;
                    cycleStep = 0;
                }
                else
                {
                    cycleStep++;
                }
            }

            return result;
        }
    }


    // Structured type boundary value generator
    public class SteStructuredTypeBoundaries : SteTypeBoundaries
    {
        private class SteStructuredTypeBoundariesEnumerator : IEnumerator<StePermutation>
        {
            private enum LogicalPosition
            {
                BeforeElements,          // Position is prior to first element and there is at least one element
                OnElement,         // Position is on an element
                AfterElements             // Position is after final element
            }


            // Value list can be static, since it'll only ever be used in one way.
            private static readonly IList<SteAttributeKey> s_valueKey = new List<SteAttributeKey>(new SteAttributeKey[] { SteAttributeKey.Value });

            private readonly SteStructuredTypeBoundaries _parent;
            private readonly bool _isMultiValued;
            private readonly IList<SteAttributeKey> _metaDataKeysOfInterest; // metadata keys that should be used
            private object[][] _separateValueList;  // use the value list separately?
            private IList<IEnumerator<StePermutation>> _fieldEnumerators; // List of enumerators over subordinate types
            private bool[] _completed;         // Which enumerators have already completed?
            private LogicalPosition _logicalPosition;   // Logical positioning of self
            private int _typeNumber;        // used to uniquely separate each type for this session
            private readonly string _typeNameBase;
            private StePermutation _current;
            private readonly StePermutation _rowCountColumn;

            public SteStructuredTypeBoundariesEnumerator(
                            SteStructuredTypeBoundaries parent, IEnumerable<SteAttributeKey> keysOfInterest, bool isMultiValued)
            {
                _parent = parent;
                _typeNameBase = "SteStructType" + Guid.NewGuid();
                _isMultiValued = isMultiValued;
                _current = null;

                // Separate values from everything else, so we can generate a complete table per permutation based on said values.
                bool usesValues = false;
                _metaDataKeysOfInterest = new List<SteAttributeKey>();
                foreach (SteAttributeKey key in keysOfInterest)
                {
                    if (SteAttributeKey.Value == key)
                    {
                        usesValues = true;
                    }
                    else
                    {
                        _metaDataKeysOfInterest.Add(key);
                    }
                }

                if (usesValues)
                {
                    if (_isMultiValued)
                    {
                        CreateSeparateValueList();
                    }
                    else
                    {
                        _metaDataKeysOfInterest.Add(SteAttributeKey.Value);
                    }
                }

                // set up rowcount column
                _rowCountColumn = new StePermutation();
                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.SqlDbType))
                {
                    _rowCountColumn.Add(SteAttributeKey.SqlDbType, SqlDbType2.Int);
                }

                Reset();
            }

            public StePermutation Current
            {
                get
                {
                    return _current;
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _logicalPosition = LogicalPosition.AfterElements;
                _fieldEnumerators = null;
            }

            public bool UseSeparateValueList
            {
                get
                {
                    return null != _separateValueList;
                }
            }

            public object[][] SeparateValues
            {
                get
                {
                    return _separateValueList;
                }
            }

            public bool MoveNext()
            {
                bool result = false;
                if (LogicalPosition.BeforeElements == _logicalPosition)
                {
                    _logicalPosition = LogicalPosition.OnElement;
                    result = true;
                }
                else if (LogicalPosition.OnElement == _logicalPosition)
                {
                    for (int i = 0; i < _fieldEnumerators.Count; i++)
                    {
                        IEnumerator<StePermutation> field = _fieldEnumerators[i];
                        if (!field.MoveNext())
                        {
                            field.Reset();
                            field.MoveNext();
                            _completed[i] = true;
                        }
                        else if (!_completed[i])
                        {
                            result = true;
                            break;
                        }
                    }
                }

                if (result)
                {
                    if (LogicalPosition.OnElement == _logicalPosition)
                    {
                        List<StePermutation> fields = new();
                        foreach (IEnumerator<StePermutation> field in _fieldEnumerators)
                        {
                            fields.Add(field.Current);
                        }
                        fields.Add(_rowCountColumn);

                        _current = CreateTopLevelPermutation(fields);
                    }
                }


                return result;
            }

            public void Reset()
            {
                _fieldEnumerators = new List<IEnumerator<StePermutation>>();
                foreach (SteSimpleTypeBoundaries columnBounds in _parent.ColumnTypes)
                {
                    IEnumerator<StePermutation> fieldPermutations = columnBounds.GetEnumerator(_metaDataKeysOfInterest);

                    // Ignore empty lists
                    if (fieldPermutations.MoveNext())
                    {
                        _fieldEnumerators.Add(fieldPermutations);
                    }
                }

                if (0 < _fieldEnumerators.Count)
                {
                    _logicalPosition = LogicalPosition.BeforeElements;
                    _completed = new bool[_fieldEnumerators.Count];
                }
                else
                {
                    _logicalPosition = LogicalPosition.AfterElements;
                }
            }

            private void CreateSeparateValueList()
            {
                int childColumnCount = _parent.ColumnTypes.Count;
                int columnCount = childColumnCount + 1;
                IEnumerator<StePermutation>[] valueSources = new IEnumerator<StePermutation>[childColumnCount];
                ArrayList[] valueList = new ArrayList[childColumnCount];
                int i = 0;
                foreach (SteSimpleTypeBoundaries field in _parent.ColumnTypes)
                {
                    valueSources[i] = field.GetEnumerator(s_valueKey);
                    valueList[i] = new ArrayList();
                    i++;
                }

                // Loop over the permutation enumerators until they all complete at least once
                //  Restart enumerators that complete before the others do
                int completedColumns = 0;

                // Array to track columns that have already completed once
                bool[] isColumnComplete = new bool[childColumnCount];
                for (i = 0; i < childColumnCount; i++)
                {
                    isColumnComplete[i] = false;
                }

                // The main value-accumulation loop
                while (completedColumns < childColumnCount)
                {
                    for (i = 0; i < childColumnCount; i++)
                    {
                        if (!valueSources[i].MoveNext())
                        {
                            // update column completion, if it's the first time for this column
                            if (!isColumnComplete[i])
                            {
                                completedColumns++;
                                isColumnComplete[i] = true;
                            }

                            // restart column, and make sure there's at least one value
                            valueSources[i].Reset();
                            if (!valueSources[i].MoveNext())
                            {
                                throw new InvalidOperationException("Separate value list, but no values for column " + i);
                            }
                        }
                        valueList[i].Add(valueSources[i].Current[SteAttributeKey.Value]);
                    }
                }

                // pivot values into final list
                int rowCount = valueList[0].Count;
                object[][] separateValues = new object[rowCount][];
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    separateValues[rowIndex] = new object[columnCount];
                    for (int columnIndex = 0; columnIndex < childColumnCount; columnIndex++)
                    {
                        separateValues[rowIndex][columnIndex] = valueList[columnIndex][rowIndex];
                    }
                    separateValues[rowIndex][childColumnCount] = rowIndex;
                }
                _separateValueList = separateValues;

            }

            private StePermutation CreateTopLevelPermutation(IList<StePermutation> fields)
            {
                StePermutation perm = new();
                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.SqlDbType))
                {
                    perm.Add(SteAttributeKey.SqlDbType, SqlDbType2.Structured);
                }

                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.MultiValued))
                {
                    perm.Add(SteAttributeKey.MultiValued, _isMultiValued);
                }

                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.MaxLength))
                {
                    perm.Add(SteAttributeKey.MaxLength, -1);
                }

                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.TypeName))
                {
                    perm.Add(SteAttributeKey.TypeName, _typeNameBase + _typeNumber);
                    _typeNumber++;
                }

                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.Fields))
                {
                    perm.Add(SteAttributeKey.Fields, fields);
                }

                if (0 <= _metaDataKeysOfInterest.IndexOf(SteAttributeKey.Value))
                {
                    if (!UseSeparateValueList)
                    {
                        throw new NotSupportedException("Integrated values not yet supported by test framework.");
                    }

                    perm.Add(SteAttributeKey.Value, _separateValueList);
                }

                return perm;
            }
        }

        // class members
        public static readonly IList<SteStructuredTypeBoundaries> AllTypes;
        public static readonly SteStructuredTypeBoundaries AllColumnTypes;
        public static readonly SteStructuredTypeBoundaries AllColumnTypesExceptUdts;
        public static readonly SteStructuredTypeBoundaries UdtsOnly;
        static SteStructuredTypeBoundaries()
        {
            AllColumnTypesExceptUdts = new SteStructuredTypeBoundaries(SteSimpleTypeBoundaries.s_allTypesExceptUdts, true);
            AllColumnTypes = new SteStructuredTypeBoundaries(SteSimpleTypeBoundaries.s_allTypes, true);
            UdtsOnly = new SteStructuredTypeBoundaries(SteSimpleTypeBoundaries.s_udtsOnly, true);

            AllTypes = new List<SteStructuredTypeBoundaries>
            {
                AllColumnTypes,
                AllColumnTypesExceptUdts,
                UdtsOnly
            };
        }

        // instance fields
        private readonly IList<SteSimpleTypeBoundaries> _columnTypes;
        private readonly bool _isMultiValued;

        // ctor
        public SteStructuredTypeBoundaries(IList<SteSimpleTypeBoundaries> columnTypes, bool isMultiValued)
        {
            _columnTypes = columnTypes;
            _isMultiValued = isMultiValued;
        }

        private IList<SteSimpleTypeBoundaries> ColumnTypes
        {
            get
            {
                return _columnTypes;
            }
        }

        public override IEnumerable<SteAttributeKey> DefaultKeys
        {
            get
            {
                List<SteAttributeKey> result = new();
                foreach (SteSimpleTypeBoundaries column in _columnTypes)
                {
                    foreach (SteAttributeKey columnKey in column.DefaultKeys)
                    {
                        if (0 > result.IndexOf(columnKey))
                        {
                            result.Add(columnKey);
                        }
                    }
                }

                if (0 > result.IndexOf(SteAttributeKey.SqlDbType))
                {
                    result.Add(SteAttributeKey.SqlDbType);
                }
                if (0 > result.IndexOf(SteAttributeKey.Value))
                {
                    result.Add(SteAttributeKey.Value);
                }
                if (0 > result.IndexOf(SteAttributeKey.MaxLength))
                {
                    result.Add(SteAttributeKey.MaxLength);
                }
                if (0 > result.IndexOf(SteAttributeKey.TypeName))
                {
                    result.Add(SteAttributeKey.TypeName);
                }
                if (0 > result.IndexOf(SteAttributeKey.Fields))
                {
                    result.Add(SteAttributeKey.Fields);
                }
                return result;
            }
        }

        public override IEnumerator<StePermutation> GetEnumerator(IEnumerable<SteAttributeKey> keysOfInterest)
        {
            return new SteStructuredTypeBoundariesEnumerator(this, keysOfInterest, _isMultiValued);
        }

        public static object[][] GetSeparateValues(IEnumerator<StePermutation> enumerator)
        {
            if (enumerator is SteStructuredTypeBoundariesEnumerator myEnum)
            {
                return myEnum.SeparateValues;
            }
            else
            {
                return null;
            }
        }
    }
}
