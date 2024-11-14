package kaiwudb

type LoadingOptions struct {
	User     string
	Pass     string
	Host     []string
	Port     []int
	Buffer   int
	DBName   string
	Workers  int
	DoCreate bool
	Type     string
}
