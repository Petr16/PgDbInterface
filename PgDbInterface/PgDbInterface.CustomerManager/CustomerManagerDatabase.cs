using System.Data.Common;

namespace PgDbInterface.CustomerManager
{
    /// <summary>
    /// API для работы с БД CustomerManagerBD
    /// </summary>
    public class CustomerManagerDatabase : Database
    {
        private CustomerManagerUtilsSchema _customerManagerUtils;
        private CustomSchema _customSchema;

        public CustomerManagerUtilsSchema CustomerManagerUtils => _customerManagerUtils ??= new CustomerManagerUtilsSchema(DBConnection);
        public CustomSchema CustomSchema => _customSchema ??= new CustomSchema(DBConnection);

        public CustomerManagerDatabase(DbConnection connection) : base(connection)
        {
        }
    }
}
