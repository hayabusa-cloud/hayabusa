package plugins

type Interface interface {
	Logger
	MasterData() MasterDataManager
	Cache(id string) *Cache
	Redis(id string) *Redis
	Mongo(id string) *Mongo
	MySQL(id string) *MySQL
	Sqlite(id string) *Sqlite
	LBClient(id string) *LBClient
}
