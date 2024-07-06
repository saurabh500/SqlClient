// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Microsoft.Data.SqlClient.FeaturesX
{
    internal class SqlNegotiatedFeatures
    {
        internal readonly SRecoveryFeature SessionRecovery;
        internal readonly FedAuthFeature FedAuth;
        internal readonly TceFeature ColumnEncryption;
        internal readonly GlobalTransactionsFeature GlobalTransactions;
        internal readonly AzureSqlSupportFeature AzureSqlSupport;
        internal readonly DataClassificationFeature DataClassification;
        internal readonly Utf8SupportFeature Utf8Support;
        internal readonly SqlDnsCachingFeature SqlDnsCaching;

        public SqlNegotiatedFeatures()
        {
            SessionRecovery = new SRecoveryFeature();
            FedAuth = new FedAuthFeature();
            ColumnEncryption = new TceFeature();
            GlobalTransactions = new GlobalTransactionsFeature();
            AzureSqlSupport = new AzureSqlSupportFeature();
            DataClassification = new DataClassificationFeature();
            Utf8Support = new Utf8SupportFeature();
            SqlDnsCaching = new SqlDnsCachingFeature();
        }
    }

    internal abstract class Feature
    {
        
        internal byte FeatureId { get; private set; }
        public bool IsAcknowledged { get; protected set; }
        internal int FeatureVersion { get; private set; }
        internal bool IsRequested { get; set; }

        protected Feature(byte featureId)
        {
            FeatureId = featureId;
        }

        internal void SetAcknowledged()
        {
            IsAcknowledged = true;
        }

        internal void SetFeatureVersion(int featureVersion)
        {
            FeatureVersion = featureVersion;
        }
    }

    internal class SRecoveryFeature : Feature
    {
        public SRecoveryFeature() : base(TdsEnums.FEATUREEXT_SRECOVERY)
        {
        }
    }

    internal class FedAuthFeature : Feature
    {
        public FedAuthFeature() : base(TdsEnums.FEATUREEXT_FEDAUTH)
        {
        }

        internal bool IsInfoRequested { get; set; }
        
        internal bool IsInfoReceived { get; set; }
    }

    internal class TceFeature : Feature
    {
        public TceFeature() : base(TdsEnums.FEATUREEXT_TCE)
        {
        }

        public bool AreEnclaveRetriesSupported => FeatureVersion == 3;

        /// <summary>
        /// Type of enclave being used by the server
        /// </summary>
        internal string EnclaveType { get; set; }
    }

    internal class GlobalTransactionsFeature : Feature
    {
        public GlobalTransactionsFeature() : base(TdsEnums.FEATUREEXT_GLOBALTRANSACTIONS)
        {
        }

        public bool IsEnabledOnServer { get; private set; }

        internal void SetEnabledOnServer()
        {
            IsEnabledOnServer = true;
        }
    }

    internal class AzureSqlSupportFeature : Feature
    {
        public AzureSqlSupportFeature() : base(TdsEnums.FEATUREEXT_AZURESQLSUPPORT)
        {
        }
    }

    internal class DataClassificationFeature : Feature
    {
        public DataClassificationFeature() : base(TdsEnums.FEATUREEXT_DATACLASSIFICATION)
        {
        }
    }

    internal class Utf8SupportFeature : Feature
    {
        public Utf8SupportFeature() : base(TdsEnums.FEATUREEXT_UTF8SUPPORT)
        {
        }
    }

    internal class SqlDnsCachingFeature : Feature
    {
        public SqlDnsCachingFeature() : base(TdsEnums.FEATUREEXT_SQLDNSCACHING)
        {
        }

        public bool IsDNSCachingBeforeRedirectSupported { get; internal set; }

        internal void Reset()
        {
            IsAcknowledged = false;
        }
    }
}
