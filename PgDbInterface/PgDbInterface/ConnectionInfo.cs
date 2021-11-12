namespace PgDbInterface
{
    public class ConnectionInfo
    {
        private string _name;

        public string Name
        {
            get
            {
                return _name ?? (Host + (DbName != null ? "/" + DbName : null));
            }
            set
            {
                _name = value;
            }
        }

        public string User { get; set; }

        public string Password { get; set; }

        public string DbName { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public ConnectionInfo()
        {
            Port = 5432;
        }

        public ConnectionInfo(string name, string user, string password, string host, string dbName, int port = 5432)
        {
            _name = name;
            User = user;
            Password = password;
            Host = host;
            DbName = dbName;
            Port = port;
        }

        public override string ToString()
        {
            return Name;
        }
    }
}
