using System;
using System.Collections.Generic;
using System.Text;

namespace DgraphNet.Client
{
    public class DgraphException : Exception
    {
        internal DgraphException(string message) : base(message)
        {
        }
    }

    public abstract class TxnException : Exception
    {
        internal TxnException(string message) : base(message)
        {
        }
    }

    public class TxnFinishedException : TxnException
    {
        internal TxnFinishedException() : base("Transaction has already been committed or discarded")
        {
        }
    }

    public class TxnConflictException : TxnException
    {
        public TxnConflictException(string msg) : base(msg)
        {
        }
    }
}
