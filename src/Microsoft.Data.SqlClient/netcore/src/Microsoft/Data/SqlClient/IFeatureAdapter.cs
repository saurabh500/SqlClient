// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Data.SqlClient
{
    internal interface IFeatureAdapter
    {
        bool AreEnclaveRetriesSupported { get; }

        bool IsColumnEncryptionSupported { get; }

        int? TceVersionSupported { get;  }

        string EnclaveType { get;  }

        int DataClassificationVersion { get; }
    }
}
