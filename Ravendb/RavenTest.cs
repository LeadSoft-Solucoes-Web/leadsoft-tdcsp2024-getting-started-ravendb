using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Ravendb.entities;

namespace Ravendb
{
    public class DatabaseFixture : IDisposable
    {
        // Singleton
        public IDocumentStore Store { get; private set; }

        public DatabaseFixture()
        {
            Store = new DocumentStore
            {
                Urls = new[]
                {
                    "http://localhost:8080",
                    "http://localhost:8081",
                    "http://localhost:8082"
                },
                Database = "LeadSoft",
                Conventions = {}
            }.Initialize();
        }

        public void Dispose()
        {
            Store.Dispose();
        }
    }

    public class RavenTest : IClassFixture<DatabaseFixture>
    {
        DatabaseFixture databaseFixture;

        public RavenTest(DatabaseFixture fixture)
        {
            databaseFixture = fixture;
        }

        [Fact]
        public async Task Storing()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                Category category = new Category
                {
                    Name = "Database Category",
                };

                await asyncSession.StoreAsync(category);                        // Assign an 'Id' and collection (Categories)
                                                                                // and start tracking an entity

                Product product = new Product
                {
                    Name = "RavenDB database",
                    Category = category.Id,
                    UnitsInStock = 10
                };

                await asyncSession.StoreAsync(product);                         // Assign an 'Id' and collection (Products)
                                                                                // and start tracking an entity

                await asyncSession.SaveChangesAsync();                          // Send to the Server
                                                                                // one request processed in one transaction
            }
        }

        [Fact]
        public async Task BulkInsert()
        {
            BulkInsertOperation bulkInsert = null;
            try
            {
                bulkInsert = databaseFixture.Store.BulkInsert();
                List<Category> categories = new List<Category>();

                for (int i = 0; i < 500; i++)
                {
                    Category category = new Category
                    {
                        Name = "Category #" + i
                    };
                    categories.Add(category);
                    await bulkInsert.StoreAsync(category);
                }

                Random rnd = new Random();
                for (int i = 0; i < 2000 * 1000; i++)
                {
                    int randomIndex = rnd.Next(0, 500);
                    await bulkInsert.StoreAsync(new Product
                    {
                        Name = "Product #" + i,
                        UnitsInStock = randomIndex,
                        Category = categories[randomIndex].Id
                    });
                }
            }
            finally
            {
                if (bulkInsert != null)
                {
                    await bulkInsert.DisposeAsync();
                }
            }
        }

        [Fact]
        public async Task Loading()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                string productId = "products/1-A";

                Product product = await asyncSession
                    .Include<Product>(x => x.Category)                          // Include Category
                    .LoadAsync<Product>(productId);                             // Load the Product and start tracking

                Category category = await asyncSession
                    .LoadAsync<Category>(product.Category);                     // No remote calls,
                                                                                // Session contains this entity from .Include
                Assert.Equal("RavenDB database", product.Name);
                Assert.Equal("Database Category", category.Name);

                IDictionary<string, Product> products = await asyncSession                    // Load from List of IDs
                    .LoadAsync<Product>(["products/1-A", "Product #999998", "Product #996612"]);

                Assert.True(products.ContainsKey("products/1-A"));
                Assert.True(products.ContainsKey("Product #999998"));
                Assert.True(products.ContainsKey("Product #996612"));
            }
        }

        [Fact]
        public async Task ApplyChanges()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                string productId = "products/1-A";

                Product product = await asyncSession
                    .Include<Product>(x => x.Category)                          // Include Category
                    .LoadAsync(productId);                                      // Load the Product and start tracking

                Category category = await asyncSession
                    .LoadAsync<Category>(product.Category);                     // No remote calls,
                                                                                // Session contains this entity from .Include

                product.Name = "RavenDB";                                       // Apply changes
                category.Name = "Database";

                await asyncSession.SaveChangesAsync();                          // Synchronize with the Server
                                                                                // one request processed in one transaction

                Assert.Equal("RavenDB", product.Name);
                Assert.Equal("Database", category.Name);
            }
        }

        [Fact]
        public async Task Delete()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                Product product = await asyncSession
                    .Include<Product>(x => x.Category)
                    .LoadAsync("products/2-A");

                asyncSession.Delete(product);
                await asyncSession.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task Querying()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                List<string> productNames = await asyncSession                  // Query for Products
                    .Query<Product>()
                    .Statistics(out QueryStatistics stats)
                    .Where(x => x.UnitsInStock > 5 && x.UnitsInStock < 11)      // Filter
                    .OrderByDescending(x => x.Name)                             // Order By
                    .Skip(0).Take(10)                                           // Page
                    .Select(x => x.Name)                                        // Project
                    .ToListAsync();                                             // Materialize query

                Assert.True("RavenDB".In(productNames));
            }
        }

        [Fact]
        public async Task StoringAttachment()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            using (var file1 = File.Open("rook-suit.png", FileMode.Open))
            {
                Product product = await asyncSession
                    .Include<Product>(x => x.Category)
                    .LoadAsync("products/1-A");

                asyncSession.Advanced.Attachments.Store(product.Id, "rook-suit", file1, "image/png");

                await asyncSession.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task FullTextSearch()
        {
            using (IAsyncDocumentSession asyncSession = databaseFixture.Store.OpenAsyncSession())
            {
                List<Product> products = await asyncSession
                    .Query<Product>()
                    .Search(x => x.Name, new[] { "#999996", "#999995" })
                    .ToListAsync();

                Assert.True(products.Any(p => p.Name.Contains("#999996")));
                Assert.True(products.Any(p => p.Name.Contains("#999995")));
            }
        }
    }
}