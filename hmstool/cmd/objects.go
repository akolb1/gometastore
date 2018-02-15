package cmd

const (
	partitionsType = "parts"
	tableType      = "table"
	dbType         = "db"
)

type HmsObject struct {
	Type   string
	Values []interface{}
}
