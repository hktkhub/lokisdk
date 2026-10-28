package lokisdk

// labelKey 是 Loki stream label 的维度键。
//
// 该类型不导出：维度只能在本文件声明，包外无法凭空构造。新增维度时必须在下方
// 常量表中追加，并同步在 log 的默认标签注入处接线，否则新维度不会被写入 stream。
type labelKey string

// 新增维度时：先在此追加常量，再到 log 的 emit 处完成标签注入。
const (
	// App 标识日志的生产方应用，对应维度 "app"。
	App labelKey = "app"
	// Service 标识应用内的生产服务，对应维度 "service"。
	Service labelKey = "service"
	// Env 标识部署环境，对应维度 "env"。
	Env labelKey = "env"
	// Level 标识日志级别，取值为 debug/info/warn/error/metric。
	Level labelKey = "level"
)

// Label 是一个 stream label 键值对；Key 只能取自本文件声明的维度。
type Label struct {
	Key   labelKey
	Value string
}

