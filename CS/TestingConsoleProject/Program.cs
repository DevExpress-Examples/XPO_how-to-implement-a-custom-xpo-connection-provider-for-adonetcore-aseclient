using System;
using System.Linq;
using DevExpress.Xpo;
using DevExpress.Xpo.DB;


namespace TestingConsoleProject {
    class Program {
        static void Main(string[] args) {
            string connectionString = AseClientConnectionProvider.GetConnectionString("SERVERNAME", 5000, "DATABASE", "USER", "PASSWORD") + ";TextSize=10000000;Charset=utf8;";

            using(var dataLayer = XpoDefault.GetDataLayer(connectionString, AutoCreateOption.DatabaseAndSchema)) {
                using(var uow = new UnitOfWork(dataLayer)) {
                    for(int i = 0; i < 10; i++) {
                        TestEntity entity = new TestEntity(uow);
                        entity.Data = "Data" + i.ToString();
                    }
                    uow.CommitChanges();
                }
                using(var uow = new UnitOfWork(dataLayer)) {
                    var entityCount = uow.Query<TestEntity>().Count();
                    Console.WriteLine($"Count: {entityCount}");

                    Console.WriteLine();
                    Console.WriteLine("------------ All -------------");
                    var allEntities = uow.Query<TestEntity>().ToList();
                    foreach(var entity in allEntities) {
                        Console.WriteLine($"Id: {entity.Oid}; Data: {entity.Data};");
                    }

                    Console.WriteLine();
                    Console.WriteLine("------------ e.Data.EndsWith(\"5\") -------------");
                    var entitiesEndWith5 = uow.Query<TestEntity>().Where(e => e.Data.EndsWith("5"));
                    foreach(var entity in entitiesEndWith5) {
                        Console.WriteLine($"Id: {entity.Oid}; Data: {entity.Data};");
                    }
                }
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
            }
        }
    }

    public class TestEntity : XPObject {
        string data;
        [Size(SizeAttribute.DefaultStringMappingFieldSize)]
        public string Data {
            get => data;
            set => SetPropertyValue(nameof(Data), ref data, value);
        }
        public TestEntity(Session session)
            : base(session) {
        }
    }
}
