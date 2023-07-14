package scerror

const (
	ParseConfigParamsError = "required NODE_NAME env variable is not specified"
	KubConfigError         = "config kubernetes error"
	KubCreateClientError   = "error create kubernetes client"
	ParseOutlsblkError     = "parse out lsblk error"
	ExeLSBLK               = "exec lsblk %s error out %s"
)
