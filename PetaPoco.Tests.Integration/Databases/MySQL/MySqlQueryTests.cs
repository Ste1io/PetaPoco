using Shouldly;
using System;
using System.Linq;
using Xunit;

namespace PetaPoco.Tests.Integration.Databases.MySQL
{
	[Collection("MySql")]
	public class MySqlQueryTests : BaseQueryTests
	{
		public MySqlQueryTests()
			: base(new MySqlDBTestProvider())
		{
		}

		[Fact]
		public void GetFactory_GivenTypeFieldWithNoSetMethod_ShouldThrow()
		{
			DB.Insert("BugInvestigation_670", "Id", true, new BugInvestigation_670_Source { Id = 1 });

			var sql = "SELECT * FROM BugInvestigation_670";
			Should.Throw<InvalidOperationException>(() => DB.Query<BugInvestigation_670_Mapped>(sql).ToList());
		}

		[TableName("BugInvestigation_670")]
		public class BugInvestigation_670_Source
		{
			public int Id { get; set; }
		}

		public class BugInvestigation_670_Mapped
		{
			public int Id { get; }
		}
	}
}
