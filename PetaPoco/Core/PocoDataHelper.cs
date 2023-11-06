using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using PetaPoco.Core;

namespace PetaPoco
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    public class PocoDataHelper
    {
        private readonly IDatabase _db;
        private readonly Lazy<PocoData> _pd;
        private readonly ConcurrentDictionary<string, string> _columnNameCache = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<string, string> _propToColumnMap = new ConcurrentDictionary<string, string>();

        public PocoData PocoData => _pd.Value;

        public PocoDataHelper(IDatabase database, Type pocoType, IMapper defaultMapper = null)
        {
            _db = database;
            _pd = new Lazy<PocoData>(() => PocoData.ForType(pocoType, defaultMapper ?? _db.DefaultMapper));
        }

        public PocoDataHelper(IDatabase database, object pocoObject, string primaryKeyName, IMapper defaultMapper = null)
        {
            _db = database;
            _pd = new Lazy<PocoData>(() => PocoData.ForObject(pocoObject, primaryKeyName, defaultMapper ?? _db.DefaultMapper));
        }

        public string EscapeTableName(string tableName) => _db.Provider.EscapeTableName(tableName);

        public string EscapedPrimaryKeyName() => _db.Provider.EscapeSqlIdentifier(PocoData.TableInfo.PrimaryKey);

        public string EscapeColumnName(string propertyName)
        {
            var colName = _propToColumnMap.GetOrAdd(propertyName,
                _ => PocoData.Columns.Values.First(c => c.PropertyInfo.Name.Equals(propertyName)).ColumnName);
            return _columnNameCache.GetOrAdd(colName, _db.Provider.EscapeSqlIdentifier);
        }
    }

    public class PocoDataHelper<T> : PocoDataHelper
    {
        public PocoDataHelper(IDatabase database, IMapper defaultMapper = null)
            : base(database, typeof(T), defaultMapper) { }

        public string EscapedTableName() => EscapeTableName(PocoData.TableInfo.TableName);

        public string EscapeColumnName(Expression<Func<T, object>> propertyExpression)
        {
            var memberExpr = propertyExpression.Body as MemberExpression
                ?? ((UnaryExpression)propertyExpression.Body).Operand as MemberExpression;
            var propName = memberExpr.Member.Name;
            return EscapeColumnName(propName);
        }
    }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
