using System;
using System.Collections.Generic;
using System.Linq;
using PetaPoco.Core;
using PetaPoco.Tests.Integration.Models;
using PetaPoco.Tests.Integration.Providers;
using Shouldly;
using Xunit;

namespace PetaPoco.Tests.Integration.Databases.SQLite
{
    public abstract partial class SQLiteQueryTests : QueryTests
    {
        protected SQLiteQueryTests(TestProvider provider)
            : base(provider)
        {
        }

        [Fact]
        public override void QueryMultiple_ForSingleResultsSetWithSinglePoco_ShouldReturnValidPocoCollection()
        {
            AddPeople(1, 0);

            var pd = new PocoDataHelper<Person>(DB);
            var pdName = pd.GetColumnName(c => c.Name);

            var sql = $@"SELECT *
                         FROM {pd.GetTableName()}
                         WHERE {pdName} LIKE @0 || '%';";

            List<Person> result;
            using (var multi = DB.QueryMultiple(sql, "Peta"))
            {
                result = multi.Read<Person>().ToList();
            }

            result.Count.ShouldBe(1);

            var person = result.First();
            person.Id.ShouldNotBe(Guid.Empty);
            person.Name.ShouldStartWith("Peta");
            person.Age.ShouldBe(18);
        }

        [Fact]
        public override void QueryMultiple_ForSingleResultsSetWithMultiPoco_ShouldReturnValidPocoCollection()
        {
            AddOrders(1);

            var pd = new PocoDataHelper<Person>(DB);
            var od = new PocoDataHelper<Order>(DB);
            var pdId = pd.GetKeyName();
            var pdName = pd.GetColumnName(c => c.Name);
            var odPersonId = od.GetColumnName(c => c.PersonId);

            var sql = $@"SELECT * FROM {od.GetTableName()} o
                         INNER JOIN {pd.GetTableName()} p ON p.{pdId} = o.{odPersonId}
                         WHERE p.{pdName} = @0
                         ORDER BY 1 DESC
                         LIMIT 1;";

            List<Order> result;
            using (var multi = DB.QueryMultiple(sql, "Peta0"))
            {
                result = multi.Read<Order, Person, Order>((o, p) =>
                {
                    o.Person = p;
                    return o;
                }).ToList();
            }

            result.Count.ShouldBe(1);

            var order = result.First();

            order.PoNumber.ShouldStartWith("PO");
            order.Status.ShouldBeOneOf(Enum.GetValues(typeof(OrderStatus)).Cast<OrderStatus>().ToArray());
            order.PersonId.ShouldNotBe(Guid.Empty);
            order.CreatedOn.ShouldBeLessThanOrEqualTo(new DateTime(1990, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            order.CreatedBy.ShouldStartWith("Harry");

            order.Person.ShouldNotBeNull();
            order.Person.Id.ShouldNotBe(Guid.Empty);
            order.Person.Name.ShouldStartWith("Peta");
            order.Person.Age.ShouldBe(18);
        }

        [Fact]
        public override void QueryMultiple_ForMultiResultsSetWithSinglePoco_ShouldReturnValidPocoCollection()
        {
            AddOrders(1);

            var pd = new PocoDataHelper<Person>(DB);
            var od = new PocoDataHelper<Order>(DB);
            var pdName = pd.GetColumnName(c => c.Name);
            var odId = od.GetKeyName();

            var sql = $@"SELECT * FROM {od.GetTableName()} o
                         WHERE o.{odId} = @0;
                         SELECT * FROM {pd.GetTableName()} p
                         WHERE p.{pdName} = @1;";

            Order order;
            using (var multi = DB.QueryMultiple(sql, "1", "Peta0"))
            {
                order = multi.Read<Order>().First();
                order.Person = multi.Read<Person>().First();
            }

            order.PoNumber.ShouldStartWith("PO");
            order.Status.ShouldBeOneOf(Enum.GetValues(typeof(OrderStatus)).Cast<OrderStatus>().ToArray());
            order.PersonId.ShouldNotBe(Guid.Empty);
            order.CreatedOn.ShouldBeLessThanOrEqualTo(new DateTime(1990, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            order.CreatedBy.ShouldStartWith("Harry");

            order.Person.ShouldNotBeNull();
            order.Person.Id.ShouldNotBe(Guid.Empty);
            order.Person.Name.ShouldStartWith("Peta");
            order.Person.Age.ShouldBe(18);
        }

        [Fact]
        public override void QueryMultiple_ForMultiResultsSetWithMultiPoco_ShouldReturnValidPocoCollection()
        {
            AddOrders(12);

            var pd = new PocoDataHelper<Person>(DB);
            var od = new PocoDataHelper<Order>(DB);
            var old = new PocoDataHelper<OrderLine>(DB);
            var pdId = pd.GetKeyName();
            var odId = od.GetKeyName();
            var odPersonId = od.GetColumnName(c => c.PersonId);
            var oldOrderId = old.GetColumnName(c => c.OrderId);

            var sql = $@"SELECT * FROM {od.GetTableName()} o
                         INNER JOIN {pd.GetTableName()} p ON p.{pdId} = o.{odPersonId}
                         ORDER BY o.{odId} ASC;
                         SELECT * FROM {old.GetTableName()} ol
                         ORDER BY ol.{oldOrderId} ASC;";

            List<Order> results;
            using (var multi = DB.QueryMultiple(sql))
            {
                results = multi.Read<Order, Person, Order>((o, p) =>
                {
                    o.Person = p;
                    return o;
                }).ToList();

                var orderLines = multi.Read<OrderLine>().ToList();
                foreach (var order in results)
                    order.OrderLines = orderLines.Where(ol => ol.OrderId == order.Id).ToList();
            }

            results.Count.ShouldBe(12);

            results.ForEach(o =>
            {
                o.PoNumber.ShouldStartWith("PO");
                o.Status.ShouldBeOneOf(Enum.GetValues(typeof(OrderStatus)).Cast<OrderStatus>().ToArray());
                o.PersonId.ShouldNotBe(Guid.Empty);
                o.CreatedOn.ShouldBeLessThanOrEqualTo(new DateTime(1990, 1, 1, 0, 0, 0, DateTimeKind.Utc));
                o.CreatedBy.ShouldStartWith("Harry");

                o.Person.ShouldNotBeNull();
                o.Person.Id.ShouldNotBe(Guid.Empty);
                o.Person.Name.ShouldStartWith("Peta");
                o.Person.Age.ShouldBeGreaterThanOrEqualTo(18);

                o.OrderLines.Count.ShouldBe(2);

                var firstOrderLine = o.OrderLines.First();
                firstOrderLine.Quantity.ToString().ShouldBe("1");
                firstOrderLine.SellPrice.ShouldBe(9.99m);

                var secondOrderLine = o.OrderLines.Skip(1).First();
                secondOrderLine.Quantity.ToString().ShouldBe("2");
                secondOrderLine.SellPrice.ShouldBe(19.98m);
            });
        }

        [Collection("SQLite.SystemData")]
        public class SystemData : SQLiteQueryTests
        {
            public SystemData()
                : base(new SQLiteSystemDataTestProvider())
            {
            }
        }

        [Collection("SQLite.MicrosoftData")]
        public class MicrosoftData : SQLiteQueryTests
        {
            public MicrosoftData()
                : base(new SQLiteMSDataTestProvider())
            {
            }
        }
    }
}
