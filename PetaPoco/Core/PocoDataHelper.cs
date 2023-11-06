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

        public bool EscapeIdentifiers { get; set; } = true;

        public PocoData PocoData => _pd.Value;

        public PocoDataHelper(IDatabase database, Type pocoType, IMapper defaultMapper = null)
        {
            _db = database;
            _pd = new Lazy<PocoData>(() => PocoData.ForType(pocoType, defaultMapper ?? _db.DefaultMapper));
        }

        public PocoDataHelper(IDatabase database, Type pocoType, bool escapeIdentifiers, IMapper defaultMapper = null)
            : this(database, pocoType, defaultMapper) => EscapeIdentifiers = escapeIdentifiers;

        public PocoDataHelper(IDatabase database, object pocoObject, string primaryKeyName, IMapper defaultMapper = null)
        {
            _db = database;
            _pd = new Lazy<PocoData>(() => PocoData.ForObject(pocoObject, primaryKeyName, defaultMapper ?? _db.DefaultMapper));
        }

        public PocoDataHelper(IDatabase database, object pocoObject, string primaryKeyName, bool escapeIdentifiers, IMapper defaultMapper = null)
            : this(database, pocoObject, primaryKeyName, defaultMapper) => EscapeIdentifiers = escapeIdentifiers;

        public string GetTableName(string tableName, bool? escapeIdentifier = null) => (escapeIdentifier.HasValue && escapeIdentifier.Value) || EscapeIdentifiers
            ? _db.Provider.EscapeTableName(tableName)
            : tableName;

        public string GetKeyName(bool? escapeIdentifier = null) => (escapeIdentifier.HasValue && escapeIdentifier.Value) || EscapeIdentifiers
            ? _db.Provider.EscapeSqlIdentifier(PocoData.TableInfo.PrimaryKey)
            : PocoData.TableInfo.PrimaryKey;

        public string GetColumnName(string propertyName, bool? escapeIdentifier = null)
        {
            var colName = _propToColumnMap.GetOrAdd(propertyName,
                _ => PocoData.Columns.Values.First(c => c.PropertyInfo.Name.Equals(propertyName)).ColumnName);
            return (escapeIdentifier.HasValue && escapeIdentifier.Value) || EscapeIdentifiers
                ? _db.Provider.EscapeSqlIdentifier(_columnNameCache.GetOrAdd(colName, colName))
                : _columnNameCache.GetOrAdd(colName, colName);
        }

        public void InvalidateCache()
        {
            _columnNameCache.Clear();
            _propToColumnMap.Clear();
        }
    }

    public class PocoDataHelper<T> : PocoDataHelper
    {
        public PocoDataHelper(IDatabase database, IMapper defaultMapper = null)
            : base(database, typeof(T), defaultMapper) { }

        public PocoDataHelper(IDatabase database, bool escapeIdentifiers, IMapper defaultMapper = null)
            : base(database, typeof(T), escapeIdentifiers, defaultMapper) { }

        public string GetTableName(bool? escapeIdentifier = null) => GetTableName(PocoData.TableInfo.TableName, escapeIdentifier);

        public string GetColumnName(Expression<Func<T, object>> propertyExpression, bool? escapeIdentifier = null)
        {
            var memberExpr = propertyExpression.Body as MemberExpression
                ?? ((UnaryExpression)propertyExpression.Body).Operand as MemberExpression;
            var propName = memberExpr.Member.Name;
            return GetColumnName(propName, escapeIdentifier);
        }
    }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
