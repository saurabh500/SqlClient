using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Data.SqlClientX.IO
{
    internal interface ITdsWriteTraversable
    {
        bool TryGetNextType(out WritePrimitiveType writePrimitiveType);

        int GetInt();

        short GetShort();

        long GetLong();

        string GetUnicodeString();

        string GetUtfString();

        byte GetByte();
    }

    struct TdsUnit : ITdsWriteTraversable
    {
        WritePrimitiveType primitiveType;
        private uint uintVal;
        private int intVal;
        byte byteVal;
        private long longVal;
        private short shortVal;

        public static TdsUnit FromByte(byte value)
        {
            TdsUnit byteUnit = new();
            byteUnit.primitiveType = WritePrimitiveType.Byte;
            byteUnit.byteVal = value;
            return byteUnit;
        }

        public static TdsUnit FromtInt(int value)
        {
            TdsUnit intUnit = new();
            intUnit.primitiveType = WritePrimitiveType.Byte;
            intUnit.intVal = value;
            return intUnit;
        }

        public static TdsUnit FromUint(uint value)
        {
            TdsUnit uintUnit = new();
            uintUnit.primitiveType = WritePrimitiveType.Uint;
            uintUnit.uintVal = value;
            return uintUnit;
        }

        public static TdsUnit FromLong(long longVal)
        {
            TdsUnit tdsUnit = new();
            tdsUnit.primitiveType = WritePrimitiveType.Long;
            tdsUnit.longVal = longVal;
            return tdsUnit;
        }

        public byte GetByte()
        {
            return byteVal;
        }

        public int GetInt()
        {
            return intVal;
        }

        public readonly uint GetUint()
        {
            return uintVal;
        }

        public long GetLong()
        {
            return longVal;
        }

        public short GetShort()
        {
            return shortVal;
        }

        public string GetUnicodeString()
        {
            throw new NotImplementedException();
        }

        public string GetUtfString()
        {
            throw new NotImplementedException();
        }

        public bool TryGetNextType(out WritePrimitiveType writePrimitiveType)
        {
            writePrimitiveType = primitiveType;
            return true;
        }

        internal static TdsUnit FromIntAsShort(int offset)
        {
            TdsUnit tdsUnit = new()
            {
                primitiveType = WritePrimitiveType.Short,
                shortVal = (short)(offset & 0xFFFF)
            };
            return tdsUnit;
        }
    }
}
