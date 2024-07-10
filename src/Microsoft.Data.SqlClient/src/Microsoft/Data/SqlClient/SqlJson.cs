// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Data.SqlTypes;
using System.Text.Json;

namespace Microsoft.Data.SqlClient
{
    internal class SqlJson : INullable
    {
        /// <summary>
        /// Creates a new SqlJson instance.
        /// </summary>
        public SqlJson() { }

        /// <summary>
        /// Creates a new <see cref="SqlJson" /> instance, supplying the Json value from <see cref="string" /> .
        /// </summary>
        /// <param name="json">A Json string.</param>
        public SqlJson(string json) { }

        /// <summary>
        /// Creates a <see cref="SqlJson" /> instance, from the Json represented by the <see cref="JsonDocument" /> .
        /// </summary>
        /// <param name="jsonDocument">A <see cref="JsonDocument"/> instance.</param>
        public SqlJson(JsonDocument jsonDocument) { }

        /// <summary>
        /// Indicated whether the instance represents a null <see cref="SqlJson" /> value.
        /// </summary>
        public bool IsNull => throw new System.NotImplementedException();

        /// <summary>
        /// Represents a null instance of <see cref="SqlJson"/> type.
        /// </summary>
        public static SqlJson Null { get; }

        /// <summary>
        /// Gets the string representation of the Json content of this <see cref="SqlJson"/> instance.
        /// </summary>
        public string Value { get; }
    }
}
