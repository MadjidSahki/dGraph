using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DgraphNet.Client.Extensions.Builders
{
    public static class Schema
    {
        public static IPredicateBuilder Predicate(string predicateName)
        {
            return new PredicateBuilder(predicateName);
        }

        public static IEdgeBuilder Edge(string edgeName)
        {
            return new EdgeBuilder(edgeName);
        }
    }

    class PredicateBuilder : IPredicateBuilder,
        IBuildPredicate,
        IBasicPredicateBuilder,
        IStringPredicateBuilder,
        IDateTimePredicateBuilder,
        IListPredicateBuilder<IBasicPredicateBuilder>,
        IListPredicateBuilder<IStringPredicateBuilder>,
        IListPredicateBuilder<IDateTimePredicateBuilder>
    {
        string _name;
        PredicateType _type;
        StringIndexType _stringIndexType;
        DateTimeIndexType _dateTimeIndexType;
        bool _index;
        bool _list;
        bool _count;
        bool _upsert;

        public PredicateBuilder(string name)
        {
            _name = name;
        }

        #region IPredicateBuilder

        public IBasicPredicateBuilder Bool()
        {
            _type = PredicateType.Bool;
            return this;
        }

        public IDateTimePredicateBuilder DateTime()
        {
            _type = PredicateType.DateTime;
            return this;
        }

        public IBasicPredicateBuilder Float()
        {
            _type = PredicateType.Float;
            return this;
        }

        public IBasicPredicateBuilder Geo()
        {
            _type = PredicateType.Geo;
            return this;
        }

        public IBasicPredicateBuilder Int()
        {
            _type = PredicateType.Int;
            return this;
        }

        public IBuildPredicate Password()
        {
            _type = PredicateType.Password;
            return this;
        }

        public IStringPredicateBuilder String()
        {
            _type = PredicateType.String;
            return this;
        }

        #endregion

        #region IBasicPredicateBuilder

        public IBasicPredicateBuilder Index()
        {
            _index = true;
            return this;
        }

        public IListPredicateBuilder<IBasicPredicateBuilder> List()
        {
            _list = true;
            return this;
        }

        public IBasicPredicateBuilder Count()
        {
            _count = true;
            return this;
        }

        #endregion

        #region IStringPredicateBuilder

        public IStringPredicateBuilder Index(StringIndexType type)
        {
            _index = true;
            _stringIndexType = type;
            return this;
        }

        IListPredicateBuilder<IStringPredicateBuilder> IStringPredicateBuilder.List()
        {
            _list = true;
            return this;
        }

        IStringPredicateBuilder IListPredicateBuilder<IStringPredicateBuilder>.Count()
        {
            _count = true;
            return this;
        }

        #endregion

        #region IDateTimePredicateBuilder

        public IDateTimePredicateBuilder Index(DateTimeIndexType type)
        {
            _index = true;
            _dateTimeIndexType = type;
            return this;
        }

        IListPredicateBuilder<IDateTimePredicateBuilder> IDateTimePredicateBuilder.List()
        {
            _list = true;
            return this;
        }

        IDateTimePredicateBuilder IListPredicateBuilder<IDateTimePredicateBuilder>.Count()
        {
            _count = true;
            return this;
        }

        #endregion

        #region IBuildPredicate

        public IBuildPredicate Upsert()
        {
            _upsert = true;
            return this;
        }

        public string Build()
        {
            var sb = new StringBuilder();
            sb.Append($"{_name}:");
            sb.Append(" ");

            if (_list) sb.Append("[");
            sb.Append(_type.ToString().ToLowerInvariant());
            if (_list) sb.Append("]");

            if (_index)
            {
                sb.Append(" ");
                sb.Append("@index");

                if (_type == PredicateType.String)
                {
                    var selected = Enum
                        .GetValues(typeof(StringIndexType))
                        .Cast<StringIndexType>()
                        .Where(v => _stringIndexType.HasFlag(v))
                        .Select(x => x.ToString().ToLowerInvariant());

                    var selectedStr = string.Join(", ", selected);

                    sb.Append("(");
                    sb.Append(selectedStr);
                    sb.Append(")");
                }
                else if (_type == PredicateType.DateTime)
                {
                    sb.Append("(");
                    sb.Append(_dateTimeIndexType.ToString().ToLowerInvariant());
                    sb.Append(")");
                }
                else
                {
                    sb.Append("(");
                    sb.Append(_type.ToString().ToLowerInvariant());
                    sb.Append(")");
                }
            }

            if (_count)
            {
                sb.Append(" ");
                sb.Append("@count");
            }

            if (_upsert)
            {
                sb.Append(" ");
                sb.Append("@upsert");
            }

            sb.Append(" ");
            sb.Append(".");

            return sb.ToString();
        }

        #endregion
    }

    class EdgeBuilder : IEdgeBuilder
    {
        string _name;
        bool _reverse;
        bool _count;

        public EdgeBuilder(string name)
        {
            _name = name;
        }

        public IEdgeBuilder Reverse()
        {
            _reverse = true;
            return this;
        }

        public IEdgeBuilder Count()
        {
            _count = true;
            return this;
        }

        public string Build()
        {
            var parts = new List<string>();
            parts.Add($"{_name}:");
            parts.Add("uid");

            if (_reverse) parts.Add("@reverse");
            if (_count) parts.Add("@count");

            parts.Add(".");

            return string.Join(" ", parts);
        }
    }

    public interface IPredicateBuilder
    {
        IBasicPredicateBuilder Int();
        IBasicPredicateBuilder Float();
        IStringPredicateBuilder String();
        IBasicPredicateBuilder Bool();
        IDateTimePredicateBuilder DateTime();
        IBasicPredicateBuilder Geo();
        IBuildPredicate Password();
    }


    public interface IBasicPredicateBuilder : IBuildPredicate
    {
        IBasicPredicateBuilder Index();
        IListPredicateBuilder<IBasicPredicateBuilder> List();
    }

    public interface IListPredicateBuilder<T>
    {
        T Count();
    }

    public interface IStringPredicateBuilder : IBuildPredicate
    {
        IStringPredicateBuilder Index(StringIndexType type);
        IListPredicateBuilder<IStringPredicateBuilder> List();
    }

    public interface IDateTimePredicateBuilder : IBuildPredicate
    {
        IDateTimePredicateBuilder Index(DateTimeIndexType type);
        IListPredicateBuilder<IDateTimePredicateBuilder> List();
    }

    public interface IBuildPredicate
    {
        IBuildPredicate Upsert();
        string Build();
    }

    public interface IEdgeBuilder
    {
        IEdgeBuilder Reverse();
        IEdgeBuilder Count();
        string Build();
    }

    public enum PredicateType
    {
        Int,
        Float,
        String,
        Bool,
        DateTime,
        Geo,
        Password
    }

    [Flags]
    public enum StringIndexType
    {
        Exact = 1,
        Hash = 2,
        Term = 4,
        FullText = 8,
        Trigram = 16
    }

    public enum DateTimeIndexType
    {
        Year,
        Month,
        Day,
        Hour
    }
}
