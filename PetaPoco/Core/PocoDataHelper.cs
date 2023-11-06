using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using PetaPoco.Core;

namespace PetaPoco
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    public class PocoDataHelper<T>
    {
        private readonly IDatabase _db;
        private readonly Lazy<PocoData> _pd;
        private readonly ConcurrentDictionary<string, string> _columnNameCache = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<string, string> _propToColumnMap = new ConcurrentDictionary<string, string>();

        public PocoData PocoData => _pd.Value;

        public PocoDataHelper(IDatabase database, IMapper defaultMapper = null)
        {
            _db = database;
            _pd = new Lazy<PocoData>(() => PocoData.ForType(typeof(T), defaultMapper ?? _db.DefaultMapper));
        }

        public string EscapedTableName() => _db.Provider.EscapeTableName(PocoData.TableInfo.TableName);

        public string EscapedPrimaryKeyName() => _db.Provider.EscapeSqlIdentifier(PocoData.TableInfo.PrimaryKey);

        public string EscapeColumnName(Expression<Func<T, object>> propertyExpression)
        {
            var memberExpr = propertyExpression.Body as MemberExpression
                ?? ((UnaryExpression)propertyExpression.Body).Operand as MemberExpression;
            var propName = memberExpr.Member.Name;
            var colName = _propToColumnMap.GetOrAdd(propName,
                _ => PocoData.Columns.Values.First(c => c.PropertyInfo.Name.Equals(propName)).ColumnName);
            return _columnNameCache.GetOrAdd(colName, _db.Provider.EscapeSqlIdentifier);
        }
    }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
