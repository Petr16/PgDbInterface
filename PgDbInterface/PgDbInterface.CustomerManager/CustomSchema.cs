using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface.CustomerManager
{
    /// <summary>
    /// API для работы со схемой customschema
    /// </summary>
    public class CustomSchema : BaseSchemaApi
    {
        public CustomSchema(DataBaseConnection connection) : base(connection, "customschema") //здесь имя конкретной схемы, если будет использоваться отдельно
        {
        }

        /// <summary>
        /// Синхронизировать данные при помощи метода, описанного в схеме на стороне БД
        /// </summary>
        public Task SyncData(CancellationToken cancellationToken = default)
        {
            return CallProc("sync_data", new DbParameters(), cancellationToken);
        }
    }
}
